<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:37
 */

namespace Avro\Schema;

use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOBinaryEncoder;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Exception\AvroIOException;

/**
 * Avro map schema consisting of named values of defined
 * @package Avro\Schema
 */
class AvroMapSchema extends AvroSchema {

  /**
   * @var bool true if the values of the map contains string as well, it may be a union which contain string as well
   */
  private $containsString = false;

  /**
   * @var AvroNamedSchema|AvroSchema named schema name or AvroSchema of map schema values.
   */
  private $valuesSchema;

  /**
   * @var boolean true if the named schema
   * XXX Couldn't we derive this based on whether or not
   * $this->values is a string?
   */
  private $is_values_schema_from_schemata;

  /**
   * @param string|AvroSchema $values
   * @param string $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata &$schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($values, $defaultNamespace, &$schemata = null) {
    parent::__construct(AvroSchema::MAP_SCHEMA);

    $this->is_values_schema_from_schemata = false;
    $valuesSchema = null;
    if (is_string($values) &&
      $valuesSchema = $schemata->schemaByName(new AvroName($values, null, $defaultNamespace))) {
      $this->is_values_schema_from_schemata = true;
      $this->containsString = false;
    } else {
      $valuesSchema = AvroSchema::subParse($values, $defaultNamespace, $schemata);
      if ($valuesSchema instanceof AvroPrimitiveSchema && $valuesSchema->isString()) {
        $this->containsString = true;
      } elseif ($valuesSchema instanceof AvroUnionSchema && $valuesSchema->hasString()) {
        $this->containsString = true;
      }
    }
    $this->valuesSchema = $valuesSchema;
  }

  /**
   * @return AvroSchema
   */
  public function getValuesSchema() {
    return $this->valuesSchema;
  }

  /**
   * The datum should be array (map) and it should be of the type specified on the schema and the key should be string
   * @param array $datum the array which should be checked based on the current schema
   * @return bool true if all values on the datum are valid for this type
   */
  public function isValidDatum($datum) {
    if (is_array($datum)) {
      foreach ($datum as $k => $v) {
        if (!is_string($k) || !$this->getValuesSchema()->isValidDatum($v)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Writes the map in the datum on the encoder
   * @param mixed $datum the value to be written
   * @param AvroIOBinaryEncoder $encoder the encoder which should be used for the write
   * @throws AvroIOException if there was a problem while writing the datum to then encoder
   * @throws AvroIOTypeException in case of unknown type
   */
  public function writeDatum($datum, AvroIOBinaryEncoder $encoder) {
    $datum_count = count($datum);
    if ($datum_count > 0) {
      $encoder->writeLong($datum_count);
      $valuesSchema = $this->getValuesSchema();
      foreach ($datum as $k => $v) {
        $encoder->writeString($k);
        $valuesSchema->writeDatum($v, $encoder);
      }
    }
    $encoder->writeLong(0);
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::VALUES_ATTR] = $this->is_values_schema_from_schemata
      ? $this->valuesSchema->getQualifiedName() : $this->valuesSchema->toAvro();
    if ($this->containsString && AvroPrimitiveSchema::isJavaStringType()) {
      $avro[self::JAVA_STRING_ANNOTATION] = self::JAVA_STRING_TYPE;
    }
    return $avro;
  }
}
