{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
-- Test all properties in the current module, using Template Haskell.
-- You need to have a @{-\# LANGUAGE TemplateHaskell \#-}@ pragma in
-- your module for any of these to work.

-- copied across from QuickCheck 2.13.1 
module Test.Zebra.QuickCheck (
    allProperties
  ) where
  
  
import Language.Haskell.TH
import Test.QuickCheck.Property hiding (Result)
import Test.QuickCheck.Test
import Data.Char
import Data.List
import Control.Monad

import qualified System.IO as S  

-- | List all properties in the current module.
--
-- @$'allProperties'@ has type @[('String', 'Property')]@.
--
-- 'allProperties' has the same issue with scoping as 'quickCheckAll':
-- see the note there about @return []@.
allProperties :: Q Exp
allProperties = do
  Loc { loc_filename = filename } <- location
  when (filename == "<interactive>") $ error "don't run this interactively"
  ls <- runIO (fmap lines (readUTF8File filename))
  let prefixes = map (takeWhile (\c -> isAlphaNum c || c == '_' || c == '\'') . dropWhile (\c -> isSpace c || c == '>')) ls
      idents = nubBy (\x y -> snd x == snd y) (filter (("prop_" `isPrefixOf`) . snd) (zip [1..] prefixes))
#if MIN_VERSION_template_haskell(2,8,0)
      warning x = reportWarning ("Name " ++ x ++ " found in source file but was not in scope")
#else
      warning x = report False ("Name " ++ x ++ " found in source file but was not in scope")
#endif
      quickCheckOne :: (Int, String) -> Q [Exp]
      quickCheckOne (l, x) = do
        exists <- (warning x >> return False) `recover` (reify (mkName x) >> return True)
        if exists then sequence [ [| ($(stringE $ x ++ " from " ++ filename ++ ":" ++ show l),
                                     property $(monomorphic (mkName x))) |] ]
         else return []
  [| $(fmap (ListE . concat) (mapM quickCheckOne idents)) :: [(String, Property)] |]