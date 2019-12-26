<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:37
 */

namespace Avro\Schema;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
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
  private $alreadyInSchema;

  /**
   * @param string|AvroSchema $values
   * @param string|null $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata|null $schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($values, $defaultNamespace = null, $schemata = null) {
    parent::__construct(AvroSchema::MAP_SCHEMA);
    if ($schemata === null) {
      $schemata = new AvroNamedSchemata();
    }

    $this->alreadyInSchema = false;
    $valueSchema = null;
    if (is_string($values) &&
      $valueSchema = $schemata->getSchemaByName(new AvroName($values, null, $defaultNamespace))) {
      $this->alreadyInSchema = true;
      $this->containsString = false;
    } else {
      $valueSchema = AvroSchema::subParse($values, $defaultNamespace, $schemata);
      $this->containsString = $valueSchema->hasString();
    }
    $this->valuesSchema = $valueSchema;
  }

  /**
   * @return AvroSchema
   */
  public function getValuesSchema() {
    return $this->valuesSchema;
  }

  /**
   * @return bool true if this map contains string which will be used in another map
   */
  public function hasString(): bool {
    return $this->containsString;
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
      $valueSchema = $this->getValuesSchema();
      foreach ($datum as $k => $v) {
        $encoder->writeString($k);
        $valueSchema->writeDatum($v, $encoder);
      }
    }
    $encoder->writeLong(0);
  }

  /**
   * Deserialize JSON map array value to the corresponding map object for this schema type
   * @param mixed $value the value in JSON value
   * @return mixed the result object corresponding to the schema type
   * @throws AvroException if the value is not possible for deserialization for this type
   */
  public function deserializeJson($value) {
    if (!$this->isValidDatum($value)) {
      throw new AvroException('Deserialization for schema of type map: ' . $this->__toString() . ' is not possible for: ' . json_encode($value));
    }
    $result = [];
    if (is_array($value)) {
      $valueSchemas = $this->getValuesSchema();
      foreach ($value as $key => $item) {
        $result[$key] = $valueSchemas->deserializeJson($item);
      }
    }
    return $result;
  }

  /**
   * Checks to see if the the readersSchema is compatible with the current writersSchema ($this)
   * @param AvroSchema $readersSchema other schema to be checked with
   * @return boolean true if this schema is compatible with the readersSchema supplied
   */
  public function schemaMatches(AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroMapSchema) {
      return $this->getValuesSchema()->schemaMatches($readersSchema->getValuesSchema());
    }
    return false;
  }

  /**
   * Reads array data from the decoder with the current format
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   * @param AvroSchema $readersSchema the local schema which may be different from remote schema which is being used to read the data
   * @return array the data read from the decoder based on current schema
   * @throws AvroException if the type is not known for this schema
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   * @throws AvroIOSchemaMatchException if the schema between reader and writer are not the same
   */
  public function readData(AvroIOBinaryDecoder $decoder, AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroMapSchema) {
      $items = [];
      $pairCount = $decoder->readLong();
      $writersValuesSchema = $this->getValuesSchema();
      $readersValuesSchema = $readersSchema->getValuesSchema();
      while ($pairCount !== 0) {
        if ($pairCount < 0) {
          $pairCount = -$pairCount;
          // Note: we're not doing anything with block_size other than skipping it
          $decoder->skipLong();
        }
        for ($i = 0; $i < $pairCount; $i++) {
          $key = $decoder->readString();
          $items[$key] = $writersValuesSchema->readData($decoder, $readersValuesSchema);
        }
        $pairCount = $decoder->readLong();
      }
      return $items;
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
    $pairCount = $decoder->readLong();
    $valueSchema = $this->getValuesSchema();
    while ($pairCount !== 0) {
      if ($pairCount < 0) {
        $pairCount = -$pairCount;
        // Note: we're not doing anything with block_size other than skipping it
        $decoder->skipLong();
      }
      for ($i = 0; $i < $pairCount; $i++) {
        $decoder->skipString();
        $valueSchema->skipData($decoder);
      }
      $pairCount = $decoder->readLong();
    }
  }

  /**
   * Converts the $defaultValue to the corresponding format of the value needed for this schema
   *
   * @param mixed $defaultValue the value from which the defaultValue should be generated
   *
   * @return mixed the correct format of the value
   * @throws AvroException in case of any error in the reading of data or conversion
   */
  public function readDefaultValue($defaultValue) {
    $map = [];
    $valueSchema = $this->getValuesSchema();
    foreach ($defaultValue as $key => $value) {
      $map[$key] = $valueSchema->readDefaultValue($value);
    }
    return $map;
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::VALUES_ATTR] = $this->alreadyInSchema
      ? $this->valuesSchema->getQualifiedName() : $this->valuesSchema->toAvro();
    if ($this->containsString && AvroPrimitiveSchema::isJavaStringType()) {
      $avro[self::JAVA_STRING_ANNOTATION] = self::JAVA_STRING_TYPE;
    }
    return $avro;
  }
}
