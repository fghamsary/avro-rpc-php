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
use Avro\IO\AvroIOBinaryEncoder;
use Avro\IO\AvroIODatumReader;
use Avro\IO\AvroIODatumWriter;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\AvroStringIO;
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
  private static function generate_sync_marker() {
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
   * @var AvroIODatumWriter
   */
  private $datum_writer;

  /**
   * @var AvroStringIO buffer for writing
   */
  private $buffer;

  /**
   * @var AvroIOBinaryEncoder encoder for buffer
   */
  private $buffer_encoder; // AvroIOBinaryEncoder

  /**
   * @var int count of items written to block
   */
  private $block_count;

  /**
   * @var array map of object container metadata
   */
  private $metadata;

  /**
   * @var string
   */
  private $sync_marker;

  /**
   * @param AvroIO $io
   * @param AvroIODatumWriter $datum_writer
   * @param AvroSchema $writers_schema
   * @throws AvroDataIOException
   * @throws AvroException
   * @throws AvroSchemaParseException
   * @throws AvroIOSchemaMatchException
   * @throws AvroIOException
   */
  public function __construct(AvroIO $io, AvroIODatumWriter $datum_writer, AvroSchema $writers_schema = null) {
    if (!($io instanceof AvroIO)) {
      throw new AvroDataIOException('io must be instance of AvroIO');
    }

    $this->io = $io;
    $this->encoder = new AvroIOBinaryEncoder($this->io);
    $this->datum_writer = $datum_writer;
    $this->buffer = new AvroStringIO();
    $this->buffer_encoder = new AvroIOBinaryEncoder($this->buffer);
    $this->block_count = 0;
    $this->metadata = array();

    if ($writers_schema) {
      $this->sync_marker = self::generate_sync_marker();
      $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = AvroDataIO::NULL_CODEC;
      $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = strval($writers_schema);
      $this->write_header();
    } else {
      $dfr = new AvroDataIOReader($this->io, new AvroIODatumReader());
      $this->sync_marker = $dfr->getSyncMaker();
      $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = $dfr->getMetaDataByKey(AvroDataIO::METADATA_CODEC_ATTR);

      $schema_from_file = $dfr->getMetaDataByKey(AvroDataIO::METADATA_SCHEMA_ATTR);
      $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = $schema_from_file;
      $this->datum_writer->setWritersSchema(AvroSchema::parse($schema_from_file));
      $this->seek(0, SEEK_END);
    }
  }

  /**
   * @param mixed $datum
   * @throws AvroDataIOException
   * @throws AvroException
   * @throws AvroSchemaParseException
   * @throws AvroIOTypeException
   */
  public function append($datum) {
    $this->datum_writer->write($datum, $this->buffer_encoder);
    $this->block_count++;

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
    if ($this->block_count > 0) {
      $this->encoder->writeLong($this->block_count);
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

      $this->write($this->sync_marker);
      $this->buffer->truncate();
      $this->block_count = 0;
    }
  }

  /**
   * Writes the header of the AvroIO object container
   * @throws AvroException
   * @throws AvroIOTypeException
   * @throws AvroSchemaParseException
   */
  private function write_header() {
    $this->write(AvroDataIO::magic());
    $this->datum_writer->writeData(AvroDataIO::metadataSchema(),
      $this->metadata, $this->encoder);
    $this->write($this->sync_marker);
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
