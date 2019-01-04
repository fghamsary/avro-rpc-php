<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:29
 */

namespace Avro\IO\Data;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroFile;
use Avro\IO\AvroIO;
use Avro\IO\AvroIODatumReader;
use Avro\IO\AvroIODatumWriter;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\Exception\AvroDataIOException;
use Avro\IO\Exception\AvroIOException;
use Avro\Schema\AvroSchema;

/**
 * Class AvroDataIO
 *
 * Classes handling reading and writing from and to AvroIO objects
 *
 * @package Avro\IO\Data
 */
class AvroDataIO {
  /**
   * @var int used in file header
   */
  const VERSION = 1;

  /**
   * @var int count of bytes in synchronization marker
   */
  const SYNC_SIZE = 16;

  /**
   * @var int   count of items per block, arbitrarily set to 4000 * SYNC_SIZE
   * @todo make this value configurable
   */
  const SYNC_INTERVAL = 64000;

  /**
   * @var string map key for datafile metadata codec value
   */
  const METADATA_CODEC_ATTR = 'avro.codec';

  /**
   * @var string map key for datafile metadata schema value
   */
  const METADATA_SCHEMA_ATTR = 'avro.schema';
  /**
   * @var string JSON for datafile metadata schema
   */
  const METADATA_SCHEMA_JSON = '{"type":"map","values":"bytes"}';

  /**
   * @var string codec value for NULL codec
   */
  const NULL_CODEC = 'null';

  /**
   * @var string codec value for deflate codec
   */
  const DEFLATE_CODEC = 'deflate';

  /**
   * @var array array of valid codec names
   * @todo Avro implementations are required to implement deflate codec as well,
   *       so implement it already!
   */
  private static $valid_codecs = array(self::NULL_CODEC);

  /**
   * @var AvroSchema cached version of metadata schema object
   */
  private static $metadata_schema;

  /**
   * @return string the initial "magic" segment of an Avro container file header.
   */
  public static function magic() {
    return ('Obj' . pack('c', self::VERSION));
  }

  /**
   * @return int count of bytes in the initial "magic" segment of the
   *              Avro container file header
   */
  public static function magic_size() {
    return strlen(self::magic());
  }

  /**
   * @return AvroSchema object of Avro container file metadata.
   * @throws AvroSchemaParseException
   */
  public static function metadataSchema() {
    if (self::$metadata_schema === null) {
      self::$metadata_schema = AvroSchema::parse(self::METADATA_SCHEMA_JSON);
    }
    return self::$metadata_schema;
  }

  /**
   * @param string $file_path file_path of file to open
   * @param string $mode one of AvroFile::READ_MODE or AvroFile::WRITE_MODE
   * @param string $schema_json JSON of writer's schema
   * @return AvroDataIOWriter instance of AvroDataIOWriter
   *
   * @throws AvroDataIOException if $writers_schema is not provided
   *         or if an invalid $mode is given.
   * @throws AvroException
   * @throws AvroIOException
   * @throws AvroIOSchemaMatchException
   * @throws AvroSchemaParseException
   */
  public static function open_file($file_path, $mode = AvroFile::READ_MODE, $schema_json = null) {
    $schema = $schema_json !== null ? AvroSchema::parse($schema_json) : null;

    switch ($mode) {
      case AvroFile::WRITE_MODE:
        if ($schema === null) {
          throw new AvroDataIOException('Writing an Avro file requires a schema.');
        }
        $file = new AvroFile($file_path, AvroFile::WRITE_MODE);
        $io = self::openWriter($file, $schema);
        break;
      case AvroFile::READ_MODE:
        $file = new AvroFile($file_path, AvroFile::READ_MODE);
        $io = self::openReader($file, $schema);
        break;
      default:
        throw new AvroDataIOException(
          sprintf("Only modes '%s' and '%s' allowed. You gave '%s'.",
            AvroFile::READ_MODE,
            AvroFile::WRITE_MODE, $mode
          )
        );
    }
    return $io;
  }

  /**
   * @return array array of valid codecs
   */
  private static function validCodecs() {
    return self::$valid_codecs;
  }

  /**
   * @param string $codec
   * @return boolean true if $codec is a valid codec value and false otherwise
   */
  public static function isValidCodec($codec) {
    return in_array($codec, self::validCodecs());
  }

  /**
   * @param AvroIO $io
   * @param AvroSchema $schema
   * @return AvroDataIOWriter
   * @throws AvroDataIOException
   * @throws AvroException
   * @throws AvroIOException
   * @throws AvroIOSchemaMatchException
   * @throws AvroSchemaParseException
   */
  protected static function openWriter($io, $schema) {
    $writer = new AvroIODatumWriter($schema);
    return new AvroDataIOWriter($io, $writer, $schema);
  }

  /**
   * @param AvroIO $io
   * @param AvroSchema $schema
   * @return AvroDataIOReader
   * @throws AvroDataIOException
   * @throws AvroException
   * @throws AvroIOSchemaMatchException
   * @throws AvroSchemaParseException
   */
  protected static function openReader($io, $schema) {
    $reader = new AvroIODatumReader(null, $schema);
    return new AvroDataIOReader($io, $reader);
  }

}
