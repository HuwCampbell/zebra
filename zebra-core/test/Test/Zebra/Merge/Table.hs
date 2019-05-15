{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-deprecations #-}
module Test.Zebra.Merge.Table where

import           Data.Functor.Identity (runIdentity)
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Map as Map

import           Disorder.Jack (Property, Jack, quickCheckAll, property)
import           Disorder.Jack ((===), (==>), gamble, counterexample, oneOf, listOfN, sized, chooseInt, choose)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Viking.Stream (Of(..))
import qualified Viking.Stream as Stream

import           X.Control.Monad.Trans.Either (runEitherT)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Merge.Table (UnionTableError(..), MergeRowsPerBlock(..))
import qualified Zebra.Merge.Table as Merge
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError(..))
import qualified Zebra.Table.Striped as Striped
import qualified Zebra.Table.Logical as Logical

jFileTable :: Schema.Table -> Jack Striped.Table
jFileTable schema = do
  sized $ \size -> do
    n <- chooseInt (0, 20 * (size `div` 5))
    Right x <- Striped.fromLogical schema <$> jLogical schema n
    pure x

jSplits :: Striped.Table -> Jack (NonEmpty Striped.Table)
jSplits x =
  let
    n =
      Striped.length x
  in
    if n == 0 then
      pure $ x :| []
    else do
      ix <- chooseInt (1, min n 20)

      let
        (y, z) =
          Striped.splitAt ix x

      ys <- jSplits z
      pure $ y :| toList ys

jFile :: Schema.Table -> Jack (NonEmpty Striped.Table)
jFile schema = do
  jSplits =<< jFileTable schema

jModSchema :: Schema.Table -> Jack Schema.Table
jModSchema schema =
  oneOf [
      pure schema
    , jExpandedTableSchema schema
    , jContractedTableSchema schema
    ]

unionSimple :: Cons Boxed.Vector (NonEmpty Striped.Table) -> Either String (Maybe Striped.Table)
unionSimple xss0 =
  case Striped.merges =<< traverse Striped.merges (fmap Cons.fromNonEmpty xss0) of
    Left (StripedLogicalMergeError _e) ->
--       pure $ P.trace ("StripedLogicalMergeError=" <> show e) Nothing
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right x ->
      pure $ pure x

unionList ::
      Maybe Merge.MaximumRowSize
   -> MergeRowsPerBlock
   -> Cons Boxed.Vector (NonEmpty Striped.Table)
   -> Either String (Maybe Striped.Table)
unionList msize blockRows xss0 =
  case runIdentity . runEitherT . Stream.toList . Merge.unionStriped msize blockRows $ fmap Stream.each xss0 of
    Left (UnionLogicalMergeError _e) ->
--       pure $ P.trace ("UnionLogicalMergeError=" <> show e) Nothing
      pure Nothing
    Left (UnionMergeError _e) ->
--       pure $ P.trace ("UnionMergeError=" <> show e) Nothing
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right (xs0 :> ()) ->
      case Cons.fromList xs0 of
        Nothing ->
          Left "Union returned empty stream"
        Just xs ->
          case Striped.unsafeConcat xs of
            Left err ->
              Left $ ppShow err
            Right x ->
              pure $ pure x

largeBlock :: MergeRowsPerBlock
largeBlock = MergeRowsPerBlock 1024
noMaxRows :: MergeRowsPerBlock
noMaxRows = MergeRowsPerBlock 0

prop_union_identity :: Property
prop_union_identity =
  gamble jMapSchema $ \schema ->
  gamble (jFile schema) $ \file0 ->
  either (flip counterexample False) id $ do
    let
      files =
        Cons.from2 file0 (Striped.empty schema :| [])

      Right file =
        Striped.unsafeConcat $
        Cons.fromNonEmpty file0

    x <- first ppShow $ unionList Nothing noMaxRows files
    let 
      l = Just(normalizeStriped file)
      r = fmap normalizeStriped x
    when (l /= r) 
      (P.trace ("---- failed result = " <> ppShow l <> "\n---- expected result = " <> ppShow r <> "\n----") (Right ()))
    pure $ l === r

prop_union_files_same_schema :: Property
prop_union_files_same_schema =
  gamble jMapSchema $ \schema ->
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jFile schema)) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList Nothing largeBlock files
    pure $
      fmap normalizeStriped x
      ===
      fmap normalizeStriped y

prop_union_files_empty :: Property
prop_union_files_empty =
  gamble jMapSchema $ \schema ->
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jFile schema)) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList (Just (Merge.MaximumRowSize (-1))) largeBlock files
    pure $
      isJust x ==> (Just (Striped.empty schema) === y)

prop_union_files_diff_schema :: Property
prop_union_files_diff_schema =
  counterexample "=== Schema ===" $
  gamble jMapSchema $ \schema ->
  counterexample "=== Modified Schemas ===" $
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jModSchema schema)) $ \schemas ->
  isRight (Cons.fold1M' Schema.union schemas) ==>
  counterexample "=== Files ===" $
  gamble (traverse jFile schemas) $ \files ->
  either (flip counterexample False) id $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList Nothing largeBlock files
    pure $
      fmap normalizeStriped x
      ===
      fmap normalizeStriped y

prop_union_with_max_is_submap :: Property
prop_union_with_max_is_submap =
  gamble jMapSchema $ \schema ->
  gamble (Cons.unsafeFromList <$> listOfN 1 10 (jFile schema)) $ \files ->
  gamble (choose ((-1),100)) $ \msize ->
  either (flip counterexample False) property $ do
    x0 <- first ppShow $ unionList (Just (Merge.MaximumRowSize msize)) largeBlock files
    y0 <- first ppShow $ unionSimple files
    ok <- for (liftA2 (,) x0 y0) $ \(x1, y1) -> do
      x2 <- first ppShow . Logical.takeMap =<< first ppShow (Striped.toLogical x1)
      y2 <- first ppShow . Logical.takeMap =<< first ppShow (Striped.toLogical y1)
      unless ((x2 `Map.isSubmapOf` y2)) $ do
        traceM ("Failed with maps\n\tresult = " <> show x2 <> "\n\texpected = " <> show y2 <> "\n\tMax-size = " <> show msize <> "\n--------------")
      return $ x2 `Map.isSubmapOf` y2
    return $ fromMaybe True ok

return []
tests :: IO Bool
tests =
  $quickCheckAll
