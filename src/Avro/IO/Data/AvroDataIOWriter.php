<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:32
 */

namespace Avro\IO\Data;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIO;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\AvroStringIO;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Exception\AvroDataIOException;
use Avro\IO\Exception\AvroIOException;
use Avro\Schema\AvroSchema;

/**
 * Class AvroDataIOWriter
 *
 * Writes Avro data to an AvroIO source using an AvroSchema
 *
 * @package Avro\IO\Data
 */
class AvroDataIOWriter {

  /**
   * @returns string a new, unique sync marker.
   */
  private static function generateSyncMarker() {
    // From http://php.net/manual/en/function.mt-rand.php comments
    return pack('S8',
      mt_rand(0, 0xffff), mt_rand(0, 0xffff),
      mt_rand(0, 0xffff),
      mt_rand(0, 0xffff) | 0x4000,
      mt_rand(0, 0xffff) | 0x8000,
      mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff));
  }

  /**
   * @var AvroIO object container where data is written
   */
  private $io;

  /**
   * @var AvroIOBinaryEncoder encoder for object container
   */
  private $encoder;

  /**
   * @var AvroSchema
   */
  private $writerSchema;

  /**
   * @var AvroStringIO buffer for writing
   */
  private $buffer;

  /**
   * @var AvroIOBinaryEncoder encoder for buffer
   */
  private $bufferEncoder;

  /**
   * @var int count of items written to block
   */
  private $blockCount;

  /**
   * @var array map of object container metadata
   */
  private $metadata;

  /**
   * @var string
   */
  private $syncMarker;

  /**
   * @param AvroIO $io
   * @param AvroSchema $writersSchema
   * @throws AvroDataIOException
   * @throws AvroException
   * @throws AvroSchemaParseException
   * @throws AvroIOSchemaMatchException
   * @throws AvroIOException
   */
  public function __construct(AvroIO $io, AvroSchema $writersSchema = null) {
    if (!($io instanceof AvroIO)) {
      throw new AvroDataIOException('io must be instance of AvroIO');
    }

    $this->io = $io;
    $this->encoder = new AvroIOBinaryEncoder($this->io);
    $this->buffer = new AvroStringIO();
    $this->bufferEncoder = new AvroIOBinaryEncoder($this->buffer);
    $this->blockCount = 0;
    $this->metadata = [];

    if ($writersSchema !== null) {
      $this->syncMarker = self::generateSyncMarker();
      $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = AvroDataIO::NULL_CODEC;
      $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = strval($writersSchema);
      $this->writerSchema = $writersSchema;
      $this->writeHeader();
    } else {
      $dfr = new AvroDataIOReader($this->io);
      $this->syncMarker = $dfr->getSyncMaker();
      $schemaFromFile = $dfr->getMetaDataByKey(AvroDataIO::METADATA_SCHEMA_ATTR);
      $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = $dfr->getMetaDataByKey(AvroDataIO::METADATA_CODEC_ATTR);
      $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = $schemaFromFile;
      $this->writerSchema = AvroSchema::parse($schemaFromFile);
      $this->seek(0, SEEK_END);
    }
  }

  /**
   * @param mixed $datum
   * @throws AvroDataIOException
   * @throws AvroException
   * @throws AvroIOTypeException
   */
  public function append($datum) {
    $this->writerSchema->write($datum, $this->bufferEncoder);
    $this->blockCount++;
    if ($this->buffer->length() >= AvroDataIO::SYNC_INTERVAL) {
      $this->writeBlock();
    }
  }

  /**
   * Flushes buffer to AvroIO object container and closes it.
   * @return mixed value of $io->close()
   * @throws AvroDataIOException
   * @throws AvroIOException
   * @see AvroIO::close()
   */
  public function close() {
    $this->flush();
    return $this->io->close();
  }

  /**
   * Flushes buffer to AvroIO object container.
   * @return mixed value of $io->flush()
   * @throws AvroDataIOException
   * @throws AvroIOException
   * @see AvroIO::flush()
   */
  private function flush() {
    $this->writeBlock();
    return $this->io->flush();
  }

  /**
   * Writes a block of data to the AvroIO object container.
   * @throws AvroDataIOException if the codec provided by the encoder is not supported
   * @throws AvroIOException
   * @internal Should the codec check happen in the constructor?
   *           Why wait until we're writing data?
   */
  private function writeBlock() {
    if ($this->blockCount > 0) {
      $this->encoder->writeLong($this->blockCount);
      $to_write = strval($this->buffer);
      $this->encoder->writeLong(strlen($to_write));

      if (AvroDataIO::isValidCodec(
        $this->metadata[AvroDataIO::METADATA_CODEC_ATTR])) {
        $this->write($to_write);
      } else {
        throw new AvroDataIOException(
          sprintf('codec %s is not supported',
            $this->metadata[AvroDataIO::METADATA_CODEC_ATTR]));
      }

      $this->write($this->syncMarker);
      $this->buffer->truncate();
      $this->blockCount = 0;
    }
  }

  /**
   * Writes the header of the AvroIO object container
   * @throws AvroException
   * @throws AvroIOTypeException
   * @throws AvroSchemaParseException
   */
  private function writeHeader() {
    $this->write(AvroDataIO::magic());
    AvroDataIO::metadataSchema()->write($this->metadata, $this->encoder);
    $this->write($this->syncMarker);
  }

  /**
   * @param string $bytes
   * @uses AvroIO::write()
   * @return int count of bytes written.
   * @throws AvroIOException
   */
  private function write($bytes) {
    return $this->io->write($bytes);
  }

  /**
   * @param int $offset
   * @param int $whence
   * @uses AvroIO::seek()
   * @throws AvroIOException
   */
  private function seek($offset, $whence) {
    return $this->io->seek($offset, $whence);
  }
}
