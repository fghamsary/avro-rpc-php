<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:44
 */

namespace Avro\Schema;

use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOBinaryEncoder;
use Avro\IO\AvroIOTypeException;
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
   * @param AvroNamedSchemata &$schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($schemas, $defaultNamespace, &$schemata = null) {
    parent::__construct(AvroSchema::UNION_SCHEMA);

    $this->schemaFromSchemataIndices = [];
    $schemaTypes = [];
    foreach ($schemas as $index => $schema) {
      $is_schema_from_schemata = false;
      $newSchema = null;
      if (is_string($schema) &&
        ($newSchema = $schemata->schemaByName(new AvroName($schema, null, $defaultNamespace)))) {
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
