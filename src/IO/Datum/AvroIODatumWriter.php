<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:00
 */

namespace Avro\IO;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\Schema\AvroSchema;

/**
 * Class AvroIODatumWriter
 *
 * Handles schema-specific writing of data to the encoder.
 *
 * Ensures that each datum written is consistent with the writer's schema.
 *
 * @package Avro\IO
 */
class AvroIODatumWriter {

  /**
   * Schema used by this instance to write Avro data.
   * @var AvroSchema
   */
  private $writersSchema;

  /**
   * @param AvroSchema $writers_schema
   */
  function __construct(AvroSchema $writers_schema = null) {
    $this->writersSchema = $writers_schema;
  }

  /**
   * @param AvroSchema $writersSchema The schema of the avro used for this datum writer
   */
  public function setWritersSchema(AvroSchema $writersSchema) {
    $this->writersSchema = $writersSchema;
  }

  /**
   * @param AvroSchema $writersSchema
   * @param $datum
   * @param AvroIOBinaryEncoder $encoder
   *
   * @throws AvroException
   * @throws AvroIOTypeException if $datum is invalid for $writers_schema
   * @throws AvroSchemaParseException
   */
  function writeData(AvroSchema $writersSchema, $datum, AvroIOBinaryEncoder $encoder) {
    if (!AvroSchema::is_valid_datum($writersSchema, $datum)) {
      throw new AvroIOTypeException($writersSchema, $datum);
    }

    switch ($writersSchema->type()) {
      case AvroSchema::REQUEST_SCHEMA:
        $this->write_record($writersSchema, $datum, $encoder);
        break;
      default:
        throw new AvroException(sprintf('Unknown type: %s', $writersSchema->getType()));
    }
  }

  /**
   * @param $datum
   * @param AvroIOBinaryEncoder $encoder
   *
   * @throws AvroException
   * @throws AvroIOTypeException
   * @throws AvroSchemaParseException
   */
  function write($datum, AvroIOBinaryEncoder $encoder) {
    $this->writeData($this->writersSchema, $datum, $encoder);
  }

  /**
   * @param AvroRecordSchema $writers_schema
   * @param IAvroRecordBase|array $datum
   * @param AvroIOBinaryEncoder $encoder
   *
   * @throws AvroException
   * @throws AvroIOTypeException
   * @throws AvroSchemaParseException
   */
  private function write_record(AvroRecordSchema $writers_schema, $datum, AvroIOBinaryEncoder $encoder) {
    foreach ($writers_schema->fields() as $field) {
      if ($datum instanceof IAvroRecordBase) {
        $this->writeData($field->type(), $datum->_internalGetValue($field->name()), $encoder);
      } else {
        $this->writeData($field->type(), $datum[$field->name()], $encoder);
      }
    }
  }

}
