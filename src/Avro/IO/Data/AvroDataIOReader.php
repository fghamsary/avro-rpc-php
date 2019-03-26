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
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Exception\AvroDataIOException;
use Avro\IO\Exception\AvroIOException;
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
   * @var AvroSchema
   */
  private $readersSchema;

  /**
   * @var AvroSchema
   */
  private $writersSchema;

  /**
   * @var string
   */
  private $syncMarker;

  /**
   * @var array object container metadata
   */
  private $metadata;

  /**
   * @var int count of items in block
   */
  private $blockCount;

  /**
   * @param AvroIO $io source from which to read
   * @param AvroSchema|null $readersSchema the schema which should be used to read the data from $io
   * @throws AvroDataIOException if $io is not an instance of AvroIO
   * @throws AvroException
   * @throws AvroIOSchemaMatchException
   * @throws AvroSchemaParseException if schema is not parsable
   * @uses readHeader()
   */
  public function __construct(AvroIO $io, AvroSchema $readersSchema = null) {
    if (!($io instanceof AvroIO)) {
      throw new AvroDataIOException('io must be instance of AvroIO');
    }
    $this->io = $io;
    $this->decoder = new AvroIOBinaryDecoder($this->io);
    $this->readersSchema = $readersSchema;
    $this->readHeader();
    $codec = AvroUtil::arrayValue($this->metadata, AvroDataIO::METADATA_CODEC_ATTR);
    if ($codec && !AvroDataIO::isValidCodec($codec)) {
      throw new AvroDataIOException(sprintf('Unknown codec: %s', $codec));
    }
    $this->blockCount = 0;
    $this->writersSchema = AvroSchema::parse($this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR]);
  }

  /**
   * Reads header of object container
   * @throws AvroException
   * @throws AvroSchemaParseException
   * @throws AvroIOSchemaMatchException
   * @throws AvroDataIOException if the file is not an Avro data file.
   * @throws AvroIOException
   */
  private function readHeader() {
    $this->seek(0, AvroIO::SEEK_SET);
    $magic = $this->read(AvroDataIO::magicSize());
    if (strlen($magic) < AvroDataIO::magicSize()) {
      throw new AvroDataIOException('Not an Avro data file: shorter than the Avro magic block');
    }
    if (AvroDataIO::magic() != $magic) {
      throw new AvroDataIOException(sprintf('Not an Avro data file: %s does not match %s', $magic, AvroDataIO::magic()));
    }
    $this->metadata = AvroDataIO::metadataSchema()->read($this->decoder);
    $this->syncMarker = $this->read(AvroDataIO::SYNC_SIZE);
  }

  /**
   * @internal Would be nice to implement data() as an iterator, I think
   * @return array of data from object container.
   * @throws AvroException
   */
  public function data() {
    $data = [];
    while (true) {
      if (0 == $this->blockCount) {
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
      $data[] = $this->writersSchema->read($this->decoder, $this->readersSchema);
      $this->blockCount -= 1;
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
   * @throws AvroIOException
   */
  private function seek($offset, $whence) {
    return $this->io->seek($offset, $whence);
  }

  /**
   * @uses AvroIO::read()
   * @param int $len the number of bytes to be read
   * @return string
   * @throws AvroIOException
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
   * @throws AvroIOException
   */
  private function skipSync() {
    $proposedSyncMarker = $this->read(AvroDataIO::SYNC_SIZE);
    if ($proposedSyncMarker != $this->syncMarker) {
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
    $this->blockCount = $this->decoder->readLong();
    return $this->decoder->readLong();
  }

  public function getSyncMaker() {
    return $this->syncMarker;
  }

  public function getMetaDataByKey($key) {
    return $this->metadata[$key];
  }
}
