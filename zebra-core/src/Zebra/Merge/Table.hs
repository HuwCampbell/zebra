{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fno-warn-deprecations #-}
module Zebra.Merge.Table (
    MaximumRowSize(..)

  , MergeRowsPerBlock(..)
  , UnionTableError(..)
  , renderUnionTableError

  , unionStriped
  , unionStripedWith
  ) where

import           Control.Monad.Morph (hoist, squash)
import           Control.Monad.Trans.Class (lift)

import           Data.Text as Text
import           Data.Maybe (fromJust)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed

import           GHC.Generics (Generic)

import           P

import           Viking (Stream, Of(..))
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, left)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Logical (LogicalSchemaError, LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (SchemaUnionError)
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped

 
newtype MaximumRowSize =
  MaximumRowSize {
      unMaximumRowSize :: Int64
    } deriving (Eq, Ord, Show)

newtype MergeRowsPerBlock =
  MergeRowsPerBlock {
      unMergeRowsPerBlock :: Int
    } deriving (Eq, Ord, Show)
    
data Row = 
  Row {      
      rowKey :: !Logical.Value
    , rowValue :: Logical.Value
  } deriving (Eq, Show, Generic)

instance NFData Row

instance Ord Row where
  compare (Row k1 _) (Row k2 _) = 
    compare k1 k2 

data UnionTableError =
    UnionEmptyInput
  | UnionStripedError Text
  | UnionMergeError Text
  | UnionLogicalSchemaError !LogicalSchemaError
  | UnionLogicalMergeError !LogicalMergeError
  | UnionSchemaError !SchemaUnionError
  | UnionBinaryStripedEncodeError Text
  | UnionBinaryStripedDecodeError Text
    deriving (Eq, Show, Generic)

instance NFData UnionTableError

renderUnionTableError :: UnionTableError -> Text
renderUnionTableError = \case
  UnionEmptyInput ->
    "Cannot merge empty files"
  UnionStripedError err ->
    err
  UnionMergeError err ->
    err
  UnionLogicalSchemaError err ->
    Logical.renderLogicalSchemaError err
  UnionLogicalMergeError err ->
    Logical.renderLogicalMergeError err
  UnionSchemaError err ->
    Schema.renderSchemaUnionError err
  UnionBinaryStripedEncodeError err ->
    "BinaryStripedEncodeError: " <> err
  UnionBinaryStripedDecodeError err ->
    "BinaryStripedDecodeError: " <> err

------------------------------------------------------------------------
-- General

unionSchemas :: Cons Boxed.Vector Schema.Table -> Either UnionTableError Schema.Table
unionSchemas =
  first UnionSchemaError . Cons.fold1M' Schema.union
{-# INLINABLE unionSchemas #-}

peekHead :: Monad m => Stream (Of x) m r -> EitherT UnionTableError m (x, Stream (Of x) m r)
peekHead input = do
  e <- lift $ Stream.next input
  case e of
    Left _r ->
      left UnionEmptyInput
    Right (hd, tl) ->
      pure (hd, Stream.cons hd tl)
{-# INLINABLE peekHead #-}

streamStripedAsRows ::
     Monad m
  => Int
  -> Stream (Of Striped.Table) m ()
  -> Stream (Of Row) (EitherT UnionTableError m) ()
streamStripedAsRows _num stream =
  Stream.map (uncurry Row) $
    Stream.concat $
    Stream.mapM (hoistEither . logicalPairs) $
    hoist lift 
      stream
{-# INLINABLE streamStripedAsRows #-}

-- merge streams in a binary tree fashion
mergeStreamsBinary ::
     Monad m
  => Cons Boxed.Vector (Stream (Of Row) (EitherT UnionTableError m) ())
  -> Stream (Of Row) (EitherT UnionTableError m) ()
mergeStreamsBinary kvss =
  case Cons.length kvss of
    0 ->
      pure mempty

    1 ->
      fromJust $ Cons.index 0 kvss

    2 -> do
      let mergeS s1 s2 = remapStreamEnd $ Stream.merge s1 s2
      Cons.foldl1 mergeS kvss

    n -> do
      let
        (kvss0, kvss1) = Boxed.splitAt (n `div` 2) $ Cons.toVector kvss
        kvs0 = mergeStreamsBinary $ Cons.unsafeFromVector kvss0
        kvs1 = mergeStreamsBinary $ Cons.unsafeFromVector kvss1
      mergeStreamsBinary $ Cons.from2 kvs0 kvs1
{-# INLINABLE mergeStreamsBinary #-}


remapStreamEnd :: Monad m => Stream (Of t) m (r,r) -> Stream (Of t) m r
remapStreamEnd s = 
  s >>= (\(a,b) -> return $ seq b a)
{-# INLINABLE remapStreamEnd #-}

  
isPastMaxRowSize :: Maybe MaximumRowSize -> Int64 -> Bool
isPastMaxRowSize = \case
  Nothing ->
    const False
  Just size ->
    (> unMaximumRowSize size)
{-# INLINABLE isPastMaxRowSize #-}


unionInputGroupBy ::
     Monad m
  => Schema.Table
  -> Maybe MaximumRowSize
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Row) (EitherT UnionTableError m) ()
unionInputGroupBy schema msize inputs0 = do
  let 
    compKey :: Row -> Row -> Bool
    compKey a b = (rowKey a) == (rowKey b)

  Stream.mapMaybeM (hoistEither . mergeRows msize schema) $
    Stream.filter (/= []) $
    Stream.mapped Stream.toList $
    Stream.groupBy compKey $
    mergeStreamsBinary $
    Cons.imap streamStripedAsRows inputs0


mergeRows ::
     Maybe MaximumRowSize
  -> Schema.Table
  -> [Row]
  -> Either UnionTableError (Maybe Row)
mergeRows msize (Schema.Map _ _ schemaC) rows@(x:_) = do
  mergedVal <- first UnionLogicalMergeError $ 
      Logical.mergeValues schemaC (Boxed.fromList $ fmap rowValue rows)
  
  -- doing sizing after we've already incurred the cost of merging entity faster than doing it for each unmerged value
  if isPastMaxRowSize msize $ Logical.sizeValue mergedVal then
    pure Nothing
  else
    pure $ Just $! Row (rowKey x) mergedVal
mergeRows _ _ _ = pure Nothing
{-# INLINABLE mergeRows #-}


unionStripedWith ::
     Monad m
  => Schema.Table
  -> Maybe MaximumRowSize
  -> MergeRowsPerBlock
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStripedWith schema msize blockRows inputs0 = do
  let
    fromStriped =
      splitTable blockRows . 
      Stream.mapM (hoistEither . first (UnionStripedError . Striped.renderStripedError) . Striped.transmute schema) .
      hoist lift
  
  hoist squash $
    Stream.mapM (hoistEither . first (UnionStripedError . Striped.renderStripedError) . Striped.fromLogical schema) $
    Stream.whenEmpty (Logical.empty schema) $
    chunkRows blockRows $
    unionInputGroupBy schema msize (fmap fromStriped inputs0)
{-# INLINABLE unionStripedWith #-}


unionStriped ::
     Monad m
  => Maybe MaximumRowSize
  -> MergeRowsPerBlock
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStriped msize blockRows inputs0 = do
  (heads, inputs1) <- fmap Cons.unzip . lift $ traverse peekHead inputs0
  schema <- lift . hoistEither . unionSchemas $ fmap Striped.schema heads
  unionStripedWith schema msize blockRows inputs1
{-# INLINABLE unionStriped #-}


-- | groups together the rows as per chunksize and forms a logical table from them
chunkRows ::
     Monad m
  => MergeRowsPerBlock
  -> Stream (Of Row) (EitherT UnionTableError m) ()
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
chunkRows blockRows inputs =
    Stream.map rowsToTable $
      Stream.mapped Stream.toList $ 
      Stream.chunksOf (unMergeRowsPerBlock blockRows)
        inputs
    where
        rowsToTable rows = Logical.Map $! Map.fromDistinctAscList $ fmap (\(Row k v) -> (k, v)) rows
{-# INLINABLE chunkRows #-}


-- | split the source striped table for cases where it's chunk sizes are many times bigger than our output size
-- notably ivory would do this
splitTable ::
     Monad m
  => MergeRowsPerBlock
  -> Stream (Of Striped.Table) m ()
  -> Stream (Of Striped.Table) m ()
splitTable (MergeRowsPerBlock 0) stream = stream
splitTable blockRows stream = do
  let
    tupleToList ~(a,b) = [a,b]
    chunkSize = unMergeRowsPerBlock blockRows * 2
    splitTableAt t | Striped.length t == 0 = [t]
    splitTableAt t = do
      let chunks = (Striped.length t `div` chunkSize)
      splitChunks chunks t
    splitChunks n t = case n of
      0 -> [t]
      1 -> [t]
      2 -> tupleToList $ Striped.splitAt chunkSize t
      _ ->
        uncurry (:) $ 
          second (splitChunks (n - 1)) $ 
          Striped.splitAt chunkSize t

  Stream.concat $
    Stream.map splitTableAt
      stream
{-# INLINABLE splitTable #-}

-- | convert striped table to a vector of logical key,value pairs
-- tries to be strict on the key, lazy on the value
logicalPairs :: 
     Striped.Table 
  -> Either UnionTableError (Boxed.Vector (Logical.Value, Logical.Value))
logicalPairs (Striped.Map _ k _) | Striped.lengthColumn k == 0 =
  Right Boxed.empty
logicalPairs (Striped.Map _ k v) = do
  !ks <- first (UnionStripedError . Striped.renderStripedError) $ Striped.toValues k
  vs <- first (UnionStripedError . Striped.renderStripedError) $ Striped.toValues v
  pure $ Boxed.zip ks vs
logicalPairs _ =
  Left $ UnionMergeError "Table not a Striped.Map"
{-# INLINABLE logicalPairs #-}
