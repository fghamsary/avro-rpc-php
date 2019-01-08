<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:44
 */

namespace Avro\Schema;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Exception\AvroIOException;

/**
 * Union of Avro schemas, of which values can be of any of the schema in the union.
 * @package Avro\Schema
 */
class AvroUnionSchema extends AvroSchema {

  /**
   * @var AvroSchema[]|AvroNamedSchema[] list of schemas of this union
   */
  private $schemas;

  /**
   * @var int[] list of indices of named schemas which are defined in $schemata
   */
  public $schemaFromSchemataIndices;

  /**
   * @var boolean true if the union contains string as a possible value
   */
  private $containsString = false;

  /**
   * @param AvroSchema[] $schemas list of schemas in the union
   * @param string $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata|null &$schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($schemas, $defaultNamespace, AvroNamedSchemata $schemata = null) {
    parent::__construct(AvroSchema::UNION_SCHEMA);
    if ($schemata === null) {
      $schemata = new AvroNamedSchemata();
    }
    $this->schemaFromSchemataIndices = [];
    $schemaTypes = [];
    foreach ($schemas as $index => $schema) {
      $is_schema_from_schemata = false;
      $newSchema = null;
      if (is_string($schema) &&
        ($newSchema = $schemata->getSchemaByName(new AvroName($schema, null, $defaultNamespace)))) {
        $is_schema_from_schemata = true;
      } else {
        $newSchema = self::subParse($schema, $defaultNamespace, $schemata);
        if ($newSchema instanceof AvroPrimitiveSchema && $newSchema->isString()) {
          $this->containsString = true;
        }
      }

      $schemaType = $newSchema->getType();
      if (self::isValidType($schemaType)
        && !self::isNamedType($schemaType)
        && in_array($schemaType, $schemaTypes)) {
        throw new AvroSchemaParseException(sprintf('"%s" is already in union', $schemaType));
      } elseif (AvroSchema::UNION_SCHEMA == $schemaType) {
        throw new AvroSchemaParseException('Unions cannot contain other unions');
      } else {
        $schemaTypes[] = $schemaType;
        $this->schemas[] = $newSchema;
        if ($is_schema_from_schemata) {
          $this->schemaFromSchemataIndices[] = $index;
        }
      }
    }
  }

  /**
   * @return boolean true if the union contains string
   */
  public function hasString() {
    return $this->containsString;
  }

  /**
   * @return AvroSchema[]
   */
  public function getSchemas() {
    return $this->schemas;
  }

  /**
   * @param int $index the index of the value in the union
   * @return AvroSchema the particular schema from the union for
   * the given (zero-based) index.
   * @throws AvroSchemaParseException if the index is invalid for this schema.
   */
  public function getSchemaByIndex($index) {
    if (count($this->schemas) > $index) {
      return $this->schemas[$index];
    }
    throw new AvroSchemaParseException('Invalid union schema index');
  }

  /**
   * The datum should be one of the possible schemas in this union
   * @param mixed $datum the datum which should be checked
   * @return boolean true if the datum is compatible with the current union schema
   */
  public function isValidDatum($datum) {
    foreach ($this->getSchemas() as $schema) {
      if ($schema->isValidDatum($datum)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Writes the datum with the current union interface on the encoder
   * @param mixed $datum the value to be written
   * @param AvroIOBinaryEncoder $encoder the encoder which should be used for the write
   * @throws AvroIOException in case of error while writing
   * @throws AvroIOTypeException in case the value passed is not compatible with the current union
   */
  public function writeDatum($datum, AvroIOBinaryEncoder $encoder) {
    $datum_schema_index = -1;
    $datum_schema = null;
    foreach ($this->getSchemas() as $index => $schema) {
      if ($schema->isValidDatum($datum)) {
        $datum_schema_index = $index;
        $datum_schema = $schema;
        break;
      }
    }
    if ($datum_schema === null) {
      throw new AvroIOTypeException($this, $datum);
    }
    $encoder->writeLong($datum_schema_index);
    $datum_schema->writeDatum($datum, $encoder);
  }

  /**
   * Checks to see if the the readersSchema is compatible with the current writersSchema ($this)
   * @param AvroSchema $readersSchema other schema to be checked with
   * @return boolean true if this schema is compatible with the readersSchema supplied
   */
  public function schemaMatches(AvroSchema $readersSchema) {
    // if the readers schema is also union we say they are compatible
    if ($readersSchema instanceof AvroUnionSchema) {
      return true;
    }
    // if not it the readers schema should be compatible with one of the current schemas possible in definition
    foreach ($this->getSchemas() as $schema) {
      if ($schema->schemaMatches($readersSchema)) {
        return true;
      }
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
    $schemaIndex = $decoder->readLong();
    $selectedWritersSchema = $this->getSchemaByIndex($schemaIndex);
    return $selectedWritersSchema->read($decoder, $readersSchema);
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
    $this->getSchemaByIndex($decoder->readLong())->skipData($decoder);
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
    return $this->getSchemaByIndex(0)->readDefaultValue($defaultValue);
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = [];
    foreach ($this->schemas as $index => $schema) {
      $avro [] = (in_array($index, $this->schemaFromSchemataIndices)) ?
        $schema->getQualifiedName() :
        $schema->toAvro();
    }
    return $avro;
  }
}
