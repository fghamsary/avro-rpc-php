<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:31
 */

namespace Avro\IO\Data;

use Avro\AvroUtil;
use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIO;
use Avro\IO\AvroIOBinaryDecoder;
use Avro\IO\AvroIODatumReader;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\Exception\AvroDataIOException;
use Avro\Schema\AvroSchema;

/**
 * Class AvroDataIOReader
 *
 * Reads Avro data from an AvroIO source using an AvroSchema.
 *
 * @package Avro\IO\Data
 */
class AvroDataIOReader {

  /**
   * @var AvroIO
   */
  private $io;

  /**
   * @var AvroIOBinaryDecoder
   */
  private $decoder;

  /**
   * @var AvroIODatumReader
   */
  private $datum_reader;

  /**
   * @var string
   */
  private $sync_marker;

  /**
   * @var array object container metadata
   */
  private $metadata;

  /**
   * @var int count of items in block
   */
  private $block_count;

  /**
   * @param AvroIO $io source from which to read
   * @param AvroIODatumReader $datum_reader reader that understands
   *                                        the data schema
   * @throws AvroDataIOException if $io is not an instance of AvroIO
   * @throws AvroException
   * @throws AvroIOSchemaMatchException
   * @throws AvroSchemaParseException if schema is not parsable
   * @uses read_header()
   */
  public function __construct(AvroIO $io, AvroIODatumReader $datum_reader) {

    if (!($io instanceof AvroIO)) {
      throw new AvroDataIOException('io must be instance of AvroIO');
    }

    $this->io = $io;
    $this->decoder = new AvroIOBinaryDecoder($this->io);
    $this->datum_reader = $datum_reader;
    $this->read_header();

    $codec = AvroUtil::arrayValue($this->metadata, AvroDataIO::METADATA_CODEC_ATTR);
    if ($codec && !AvroDataIO::isValidCodec($codec)) {
      throw new AvroDataIOException(sprintf('Unknown codec: %s', $codec));
    }

    $this->block_count = 0;
    // FIXME: Seems unsanitary to set writers_schema here.
    // Can't constructor take it as an argument?
    $this->datum_reader->set_writers_schema(
      AvroSchema::parse($this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR]));
  }

  /**
   * Reads header of object container
   * @throws AvroDataIOException if the file is not an Avro data file.
   * @throws AvroException
   * @throws AvroIOSchemaMatchException
   */
  private function read_header() {
    $this->seek(0, AvroIO::SEEK_SET);

    $magic = $this->read(AvroDataIO::magic_size());

    if (strlen($magic) < AvroDataIO::magic_size()) {
      throw new AvroDataIOException(
        'Not an Avro data file: shorter than the Avro magic block');
    }

    if (AvroDataIO::magic() != $magic) {
      throw new AvroDataIOException(
        sprintf('Not an Avro data file: %s does not match %s',
          $magic, AvroDataIO::magic()));
    }

    $this->metadata = $this->datum_reader->read_data(AvroDataIO::metadataSchema(),
      AvroDataIO::metadataSchema(),
      $this->decoder);
    $this->sync_marker = $this->read(AvroDataIO::SYNC_SIZE);
  }

  /**
   * @internal Would be nice to implement data() as an iterator, I think
   * @return array of data from object container.
   * @throws AvroException
   */
  public function data() {
    $data = array();
    while (true) {
      if (0 == $this->block_count) {
        if ($this->isEof()) {
          break;
        }

        if ($this->skipSync()) {
          if ($this->isEof()) {
            break;
          }
        }

        $this->readBlockHeader();
      }
      $data [] = $this->datum_reader->read($this->decoder);
      $this->block_count -= 1;
    }
    return $data;
  }

  /**
   * Closes this writer (and its AvroIO object.)
   * @uses AvroIO::close()
   */
  public function close() {
    return $this->io->close();
  }

  /**
   * @uses AvroIO::seek()
   * @param int $offset the offset from which the
   * @param $whence
   * @throws \Avro\IO\Exception\AvroIOException
   */
  private function seek($offset, $whence) {
    return $this->io->seek($offset, $whence);
  }

  /**
   * @uses AvroIO::read()
   * @param int $len the number of bytes to be read
   * @return string
   * @throws \Avro\IO\Exception\AvroIOException
   */
  private function read($len) {
    return $this->io->read($len);
  }

  /**
   * @uses AvroIO::isEof()
   */
  private function isEof() {
    return $this->io->isEof();
  }

  /**
   * @return bool
   * @throws \Avro\IO\Exception\AvroIOException
   */
  private function skipSync() {
    $proposed_sync_marker = $this->read(AvroDataIO::SYNC_SIZE);
    if ($proposed_sync_marker != $this->sync_marker) {
      $this->seek(-AvroDataIO::SYNC_SIZE, AvroIO::SEEK_CUR);
      return false;
    }
    return true;
  }

  /**
   * Reads the block header (which includes the count of items in the block
   * and the length in bytes of the block)
   * @return int length in bytes of the block.
   * @throws AvroException
   */
  private function readBlockHeader() {
    $this->block_count = $this->decoder->read_long();
    return $this->decoder->read_long();
  }

  public function getSyncMaker() {
    return $this->sync_marker;
  }

  public function getMetaDataByKey($key) {
    return $this->metadata[$key];
  }
}
