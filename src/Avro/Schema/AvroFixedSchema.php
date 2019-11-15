<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:12
 */

namespace Avro\Schema;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Exception\AvroIOException;

/**
 * AvroNamedSchema with fixed-length data values
 * @package Avro\Schema
 */
class AvroFixedSchema extends AvroNamedSchema {

  /**
   * @var int byte count of this fixed schema data value
   */
  private $size;

  /**
   * @param AvroName $name
   * @param int $size byte count of this fixed schema data value
   * @param AvroNamedSchemata|null $schemata
   * @throws AvroSchemaParseException
   */
  public function __construct(AvroName $name, $size, $schemata = null) {
    if (!is_integer($size)) {
      throw new AvroSchemaParseException('Fixed Schema requires a valid integer for "size" attribute');
    }
    parent::__construct(AvroSchema::FIXED_SCHEMA, $name, null, $schemata);
    return $this->size = $size;
  }

  /**
   * @return int byte count of this fixed schema data value
   */
  public function getSize() {
    return $this->size;
  }

  /**
   * Checks if the datum is compatible with the size specified on the schema
   * @param mixed $datum the datum to be checked
   * @return boolean true if the datum is valid
   */
  public function isValidDatum($datum) {
    return is_string($datum) && strlen($datum) === $this->getSize();
  }

  /**
   * We write directly the fixed length as everything is known for it
   * @param mixed $datum the value to be written
   * @param AvroIOBinaryEncoder $encoder the encoder which should be used for the write
   * @throws AvroIOException if there was a problem while writing the datum to then encoder
   */
  public function writeDatum($datum, AvroIOBinaryEncoder $encoder) {
    $encoder->write($datum);
  }

  /**
   * Deserialize JSON value to the corresponding object for this schema type
   * @param mixed $value the value in JSON value
   * @return mixed the result object corresponding to the schema type
   * @throws AvroException if the value is not possible for deserialization for this type
   */
  public function deserializeJson($value) {
    if (!$this->isValidDatum($value)) {
      throw new AvroException('Deserialization for schema of type fixed: ' . $this->__toString() . ' is not possible for: ' . json_encode($value));
    }
    return $value;
  }

  /**
   * Reads data from the decoder with the current format
   * @param AvroSchema $readersSchema the local schema which may be different from remote schema which is being used to read the data
   * @return mixed the data read from the decoder based on current schema
   */
  public function schemaMatches(AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroFixedSchema) {
      return
        $this->getFullname() === $readersSchema->getFullname() &&
        $this->getSize() === $readersSchema->getSize();
    }
    return false;
  }

  /**
   * Reads data from the decoder with the current format
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   * @param AvroSchema $readersSchema the local schema which may be different from remote schema which is being used to read the data
   * @return mixed the data read from the decoder based on current schema
   * @throws AvroException if the type is not known for this schema
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   */
  public function readData(AvroIOBinaryDecoder $decoder, AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroFixedSchema){
      return $decoder->read($this->getSize());
    } else {
      throw new AvroIOSchemaMatchException($this, $readersSchema);
    }
  }

  /**
   * Skips a data based on the current schema from the decoder
   *
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   *
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   * @throws AvroException in case of any error in the reading of data or conversion
   */
  public function skipData(AvroIOBinaryDecoder $decoder) {
    $decoder->skip($this->getSize());
  }

  /**
   * Converts the $defaultValue to the corresponding format of the value needed for this schema
   *
   * @param mixed $defaultValue the value from which the defaultValue should be generated
   *
   * @return mixed the correct format of the value
   */
  public function readDefaultValue($defaultValue) {
    return $defaultValue;
  }

  /**
   * @return array fixed schema is special as we really need the complete avro definition for the type and not just the name
   */
  public function getSchemaName() {
    return $this->toAvro();
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::SIZE_ATTR] = $this->size;
    return $avro;
  }
}
