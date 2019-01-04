<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:12
 */

namespace Avro\Schema;

use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOBinaryEncoder;
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
   * @param string $doc Set to null, as fixed schemas don't have doc strings
   * @param int $size byte count of this fixed schema data value
   * @param AvroNamedSchemata &$schemata
   * @throws AvroSchemaParseException
   */
  public function __construct(AvroName $name, $doc, $size, &$schemata = null) {
    if (!is_integer($size)) {
      throw new AvroSchemaParseException('Fixed Schema requires a valid integer for "size" attribute');
    }
    if (!empty($doc)) {
      throw new AvroSchemaParseException('Fixed Schema does not support "doc" attribute');
    }
    parent::__construct(AvroSchema::FIXED_SCHEMA, $name, $doc, $schemata);
    return $this->size = $size;
  }

  /**
   * @return int byte count of this fixed schema data value
   */
  public function size() {
    return $this->size;
  }

  /**
   * Checks if the datum is compatible with the size specified on the schema
   * @param mixed $datum the datum to be checked
   * @return boolean true if the datum is valid
   */
  public function isValidDatum($datum) {
    return is_string($datum) && strlen($datum) === $this->size();
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
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::SIZE_ATTR] = $this->size;
    return $avro;
  }
}
