{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Zebra.Merge.Table (
    UnionTableError(..)

  , unionList
  , unionFile
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Primitive (PrimMonad)
import           Control.Monad.ST (runST)
import           Control.Monad.Trans.Resource (MonadResource)

import qualified Data.ByteString.Builder as Builder
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed
import qualified Data.Vector.Mutable as MBoxed

import           P

import           System.IO (FilePath, IOMode(..))
import qualified System.IO as IO

import           X.Control.Monad.Trans.Either (EitherT, runEitherT, hoistEither)
import           X.Data.Vector.Ref (Ref)
import qualified X.Data.Vector.Ref as Ref
import           X.Data.Vector.Stream (Stream(..), SPEC(..))
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Serial.Binary.Block
import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Binary.File
import           Zebra.Serial.Binary.Header
import           Zebra.Table.Logical (LogicalSchemaError, LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (SchemaError)
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped
import           Zebra.X.Vector.Cons (Cons)
import qualified Zebra.X.Vector.Cons as Cons

data UnionTableError =
    UnionFileError !FileError
  | UnionStripedError !StripedError
  | UnionLogicalSchemaError !LogicalSchemaError
  | UnionLogicalMergeError !LogicalMergeError
  | UnionSchemaMismatch !Schema.Table !Schema.Table
  | UnionSchemaError !SchemaError
    deriving (Eq, Show)

data Status =
    Active
  | Complete
    deriving (Eq)

data Input m =
  Input {
      inputSchema :: !Schema.Table
    , inputStatus :: !Status
    , inputData :: !(Map Logical.Value Logical.Value)
    , inputRead :: EitherT UnionTableError m (Maybe Striped.Table)
    }

data Output m a =
  Output {
      outputWrite :: Striped.Table -> EitherT UnionTableError m ()
    , outputClose :: EitherT UnionTableError m a
    } deriving (Functor)

data Step m =
  Step {
      _stepComplete :: !(Map Logical.Value Logical.Value)
    , _stepRemaining :: !(Cons Boxed.Vector (Input m))
    }

------------------------------------------------------------------------
-- General

boxed :: m (Ref MBoxed.MVector s a) -> m (Ref MBoxed.MVector s a)
boxed =
  id

takeSchema :: Cons Boxed.Vector (Input m) -> Either UnionTableError Schema.Table
takeSchema inputs =
  let
    (schema0, schemas) =
      Cons.uncons $ fmap inputSchema inputs
  in
    case Boxed.find (/= schema0) schemas of
      Nothing ->
        pure schema0
      Just wrong ->
        Left $ UnionSchemaMismatch schema0 wrong

hasData :: Input m -> Bool
hasData =
  not . Map.null . inputData

replaceData :: Input m -> Map Logical.Value Logical.Value -> Input m
replaceData input values =
  input {
      inputData = values
    }

isComplete :: Input m -> Bool
isComplete =
  (== Complete) . inputStatus

completeInput :: Input m -> Input m
completeInput input =
  input {
      inputStatus = Complete
    , inputData = Map.empty
    }

updateInput :: Monad m => Input m -> EitherT UnionTableError m (Input m)
updateInput input = do
  case inputStatus input of
    Complete ->
      pure input
    Active ->
      if hasData input then
        pure input
      else
        inputRead input >>= \case
          Nothing ->
            pure $ completeInput input

          Just striped -> do
            logical <- firstT UnionStripedError . hoistEither $ Striped.toLogical striped
            values <- firstT UnionLogicalSchemaError . hoistEither $ Logical.takeMap logical
            pure $ replaceData input values

writeOutput :: Monad m => Output m a -> Striped.Table -> EitherT UnionTableError m ()
writeOutput output table =
  let
    n =
      Striped.length table
  in
    if n == 0 then
      pure ()
    else if n < 4096 then
      outputWrite output table
    else do
      let
        (table0, table1) =
          Striped.splitAt 4096 table

      outputWrite output table0
      writeOutput output table1

unionStep :: Monad m => Cons Boxed.Vector (Input m) -> EitherT UnionTableError m (Step m)
unionStep inputs = do
  step <- firstT UnionLogicalMergeError . hoistEither . Logical.unionStep $ fmap inputData inputs
  pure $
    Step
      (Logical.unionComplete step)
      (Cons.zipWith replaceData inputs (Logical.unionRemaining step))

unionLoop :: Monad m => Schema.Table -> Cons Boxed.Vector (Input m) -> Output m a -> EitherT UnionTableError m a
unionLoop schema inputs0 output = do
  inputs1 <- traverse updateInput inputs0
  if Cons.all isComplete inputs1 then do
    outputClose output
  else do
    Step values inputs <- unionStep inputs1

    table <- firstT UnionStripedError . hoistEither $ Striped.fromLogical schema (Logical.Map values)
    writeOutput output table

    unionLoop schema inputs output

------------------------------------------------------------------------
-- List

mkListInput :: PrimMonad m => NonEmpty Striped.Table -> EitherT UnionTableError m (Input m)
mkListInput = \case
  x :| xs -> do
    ref <- boxed $ Ref.newRef (x : xs)
    pure . Input (Striped.schema x) Active Map.empty $ do
      ys0 <- Ref.readRef ref
      case ys0 of
        [] ->
          pure Nothing
        y : ys -> do
          Ref.writeRef ref ys
          pure $ Just y

mkListOutput :: PrimMonad m => EitherT UnionTableError m (Output m [Striped.Table])
mkListOutput = do
  ref <- boxed $ Ref.newRef []

  let
    append x =
      Ref.modifyRef ref (<> [x])

    close =
      Ref.readRef ref

  pure $ Output append close

unionList :: Cons Boxed.Vector (NonEmpty Striped.Table) -> Either UnionTableError (NonEmpty Striped.Table)
unionList inputLists =
  runST $ runEitherT $ do
    inputs <- traverse mkListInput inputLists
    schema <- hoistEither $ takeSchema inputs
    output <- mkListOutput
    result <- unionLoop schema inputs output
    case result of
      [] ->
        pure $ Striped.empty schema :| []
      x : xs ->
        pure $ x :| xs

------------------------------------------------------------------------
-- File

mkStreamReader :: MonadIO m => Stream (EitherT x m) b -> EitherT x m (EitherT x m (Maybe b))
mkStreamReader (Stream step sinit) = do
  ref <- liftIO . boxed $ Ref.newRef sinit

  let
    loop _ = do
      s0 <- liftIO $ Ref.readRef ref
      step s0 >>= \case
        Stream.Yield v s -> do
          liftIO $ Ref.writeRef ref s
          pure (Just v)
        Stream.Skip s -> do
          liftIO $ Ref.writeRef ref s
          loop SPEC
        Stream.Done -> do
          pure Nothing

  pure $ loop SPEC

mkFileInput :: MonadResource m => FilePath -> EitherT UnionTableError m (Input m)
mkFileInput path = do
  (schema, stream) <- firstT UnionFileError $ readTables path
  reader <- firstT UnionFileError $ mkStreamReader stream
  pure . Input schema Active Map.empty $
    firstT UnionFileError reader

-- FIXME MonadResource
mkFileOutput :: MonadIO m => Schema.Table -> FilePath -> EitherT UnionTableError m (Output m ())
mkFileOutput schema path = do
  hout <- liftIO $ IO.openBinaryFile path WriteMode
  liftIO . Builder.hPutBuilder hout . bHeader $ HeaderV3 schema

  pure $ Output
      (liftIO . Builder.hPutBuilder hout . bRootTableV3)
      (liftIO $ IO.hClose hout)

unionFile :: MonadResource m => Cons Boxed.Vector FilePath -> FilePath -> EitherT UnionTableError m ()
unionFile inputPaths outputPath = do
  inputs <- traverse mkFileInput inputPaths
  schema <- hoistEither $ takeSchema inputs
  output <- mkFileOutput schema outputPath

  unionLoop schema inputs output