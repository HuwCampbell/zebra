{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
module Zebra.Command.Export (
    Export(..)
  , ExportOutput(..)
  , zebraExport

  , ExportError(..)
  , renderExportError
  ) where

import           Control.Monad.IO.Class (MonadIO(..))
import           Control.Monad.Trans.Resource (MonadResource)

import qualified Data.ByteString as ByteString
import           Data.List.NonEmpty (NonEmpty)

import           P

import           System.IO (FilePath, IOMode(..), Handle, stdout)

import           X.Control.Monad.Trans.Either (EitherT, hoistEither, joinEitherT)
import           X.Data.Vector.Stream (Stream)
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Serial.Binary.File
import           Zebra.Serial.Text
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


data Export =
  Export {
      exportInput :: !FilePath
    , exportOutputs :: !(NonEmpty ExportOutput)
    } deriving (Eq, Ord, Show)

data ExportOutput =
    ExportTextStdout
  | ExportText !FilePath
  | ExportSchemaStdout
  | ExportSchema !FilePath
    deriving (Eq, Ord, Show)

data ExportError =
    ExportFileError !FileError
  | ExportTextStripedEncodeError !TextStripedEncodeError
    deriving (Eq, Show)

renderExportError :: ExportError -> Text
renderExportError = \case
  ExportFileError err ->
    renderFileError err
  ExportTextStripedEncodeError err ->
    renderTextStripedEncodeError err

zebraExport :: MonadResource m => Export -> EitherT ExportError m ()
zebraExport export = do
  (schema, tables) <- firstT ExportFileError $ readTables (exportInput export)

  for_ (exportOutputs export) $ \case
    ExportTextStdout ->
      writeText "<stdout>" stdout tables

    ExportText path -> do
      (close, handle) <- firstT ExportFileError $ openFile path WriteMode
      writeText path handle tables
      firstT ExportFileError close

    ExportSchemaStdout ->
      writeSchema "<stdout>" stdout schema

    ExportSchema path -> do
      (close, handle) <- firstT ExportFileError $ openFile path WriteMode
      writeSchema path handle schema
      firstT ExportFileError close

writeText ::
     MonadResource m
  => FilePath
  -> Handle
  -> Stream (EitherT FileError m) Striped.Table
  -> EitherT ExportError m ()
writeText path handle tables =
  joinEitherT id .
    firstT ExportFileError .
    hPutStream path handle .
    Stream.mapM (hoistEither . first ExportTextStripedEncodeError . encodeStriped) $
    Stream.trans (firstT ExportFileError) tables

writeSchema :: MonadIO m => FilePath -> Handle -> Schema.Table -> EitherT ExportError m ()
writeSchema path handle schema =
  tryIO (ExportFileError . FileWriteError path) $
    ByteString.hPut handle (encodeSchema TextV0 schema)