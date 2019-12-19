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
      $isInSchemata = false;
      $newSchema = null;
      if (is_string($schema) &&
        ($newSchema = $schemata->getSchemaByName(new AvroName($schema, null, $defaultNamespace)))) {
        $isInSchemata = true;
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
        if ($isInSchemata) {
          $this->schemaFromSchemataIndices[] = $index;
        }
      }
    }
  }

  /**
   * @return boolean true if the union contains string
   */
  public function hasString(): bool {
    return $this->containsString;
  }

  /**
   * @return AvroSchema[]
   */
  public function getSchemas(): array {
    return $this->schemas;
  }

  /**
   * @param int $index the index of the value in the union
   * @return AvroSchema the particular schema from the union for
   * the given (zero-based) index.
   * @throws AvroSchemaParseException if the index is invalid for this schema.
   */
  public function getSchemaByIndex($index): AvroSchema {
    if (count($this->schemas) > $index) {
      return $this->schemas[$index];
    }
    throw new AvroSchemaParseException('Invalid union schema index');
  }

  /**
   * Returns true if this union can contain null as a value
   * @return bool true if this union is nullable with other types as well
   */
  public function isNullable(): bool {
    return count(array_filter($this->getSchemas(), function($schema) {
      /** @var AvroSchema $schema */
      return $schema instanceof AvroPrimitiveSchema && $schema->isNull();
    })) === 1;
  }

  /**
   * Returns the type of nullable value if it's a nullable type union
   * @return AvroSchema|null the other type other than null in this union
   */
  public function getNullableSchema(): ?AvroSchema {
    if (count($this->getSchemas()) === 2 && $this->isNullable()) {
      $nullableSchema = array_filter($this->getSchemas(), function ($innerItem) {
        return !($innerItem instanceof AvroPrimitiveSchema && $innerItem->isNull());
      });
      return count($nullableSchema) === 1 ? array_values($nullableSchema)[0] : null;
    }
    return null;
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
    $datumSchemaIndex = -1;
    $datumSchema = null;
    foreach ($this->getSchemas() as $index => $schema) {
      if ($schema->isValidDatum($datum)) {
        $datumSchemaIndex = $index;
        $datumSchema = $schema;
        break;
      }
    }
    if ($datumSchema === null) {
      throw new AvroIOTypeException($this, $datum);
    }
    $encoder->writeLong($datumSchemaIndex);
    $datumSchema->writeDatum($datum, $encoder);
  }

  /**
   * Deserialize to appropriate object based on the supported values in the current Enum
   * Only nullable object of another type is possible and correct value
   * @param mixed $value the JSON value
   * @return mixed the result of the deserialization
   * @throws AvroException if the value is not possible for deserialization for this type
   */
  public function deserializeJson($value) {
    $nullable = $this->isNullable();
    // null is only possible if one of the union parts is null
    if ($value === null && $nullable) {
      return null;
    }
    $schemas = $this->getSchemas();
    $schemaRecordCounts = [];
    $possibleResults = [];
    foreach ($schemas as $schema) {
      try {
        $record = $schema->deserializeJson($value);
        $possibleResults[] = $record;
        if ($schema instanceof AvroRecordSchema) {
          $schemaRecordCounts[count($possibleResults) - 1] = count($record);
        }
      } catch (AvroException $exp) {
        // if it's not possible to deserialize to this type we check other types of union
      }
    }
    // if only one match is found we use it
    if (count($possibleResults) === 1) {
      return $possibleResults[0];
    }
    // if more than one possible value deserialization is possible we get the best guess
    // in case of multiple classes the class with more set values and/or complete
    // this can be used for inheritance cases
    if (count($possibleResults) > 1) {
      if (count($schemaRecordCounts) > 1) {
        $bestChoiceIndex = array_keys($schemaRecordCounts, max($schemaRecordCounts));
        return $possibleResults[$bestChoiceIndex[0]];
      }
      return $possibleResults[0]; // in case we couldn't determine the best choice we use the first
    }
    throw new AvroException('Deserialization is not possible for value: ' .
      json_encode($value) .
      ' as a union for: ' .
      $this->__toString());
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
