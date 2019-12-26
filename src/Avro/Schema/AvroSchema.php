<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:08
 */

namespace Avro\Schema;

use Avro\AvroUtil;
use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Exception\AvroIOException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;

/**
 * Base class for schema definition and each element of avro
 *
 * Class AvroSchema
 * @package Avro\Schema
 */
abstract class AvroSchema {

  /**
   * @var int lower bound of integer values: -(1 << 31)
   */
  const INT_MIN_VALUE = -2147483648;

  /**
   * @var int upper bound of integer values: (1 << 31) - 1
   */
  const INT_MAX_VALUE = 2147483647;

  /**
   * @var integer long lower bound of long values: -(1 << 63)
   */
  const LONG_MIN_VALUE = -9223372036854775808;

  /**
   * @var integer long upper bound of long values: (1 << 63) - 1
   */
  const LONG_MAX_VALUE = 9223372036854775807;

  /**
   * @var string null schema type name
   */
  const NULL_TYPE = 'null';

  /**
   * @var string boolean schema type name
   */
  const BOOLEAN_TYPE = 'boolean';

  /**
   * int schema type value is a 32-bit signed int
   * @var string int schema type name.
   */
  const INT_TYPE = 'int';

  /**
   * long schema type value is a 64-bit signed int
   * @var string long schema type name
   */
  const LONG_TYPE = 'long';

  /**
   * float schema type value is a 32-bit IEEE 754 floating-point number
   * @var string float schema type name
   */
  const FLOAT_TYPE = 'float';

  /**
   * double schema type value is a 64-bit IEEE 754 floating-point number
   * @var string double schema type name
   */
  const DOUBLE_TYPE = 'double';

  /**
   * string schema type value is a Unicode character sequence
   * @var string string schema type name
   */
  const STRING_TYPE = 'string';

  /**
   * string schema type value is a Unicode character sequence
   * @var string string schema type name
   */
  const JAVA_STRING_ANNOTATION = 'avro.java.string';

  /**
   * string schema type value is a Unicode character sequence
   * @var string string schema type name
   */
  const JAVA_STRING_TYPE = 'String';

  /**
   * bytes schema type value is a sequence of 8-bit unsigned bytes
   * @var string bytes schema type name
   */
  const BYTES_TYPE = 'bytes';

  // Complex Types
  // Unnamed Schema
  /**
   * @var string array schema type name
   */
  const ARRAY_SCHEMA = 'array';

  /**
   * @var string map schema type name
   */
  const MAP_SCHEMA = 'map';

  /**
   * @var string union schema type name
   */
  const UNION_SCHEMA = 'union';

  /**
   * Unions of error schemas are used by Avro messages
   * @var string error_union schema type name
   */
  const ERROR_UNION_SCHEMA = 'error_union';

  // Named Schema

  /**
   * @var string enum schema type name
   */
  const ENUM_SCHEMA = 'enum';

  /**
   * @var string fixed schema type name
   */
  const FIXED_SCHEMA = 'fixed';

  /**
   * @var string record schema type name
   */
  const RECORD_SCHEMA = 'record';
  // Other Schema

  /**
   * @var string error schema type name
   */
  const ERROR_SCHEMA = 'error';

  /**
   * @var string request schema type name
   */
  const REQUEST_SCHEMA = 'request';


  // Schema attribute names
  /**
   * @var string schema type name attribute name
   */
  const TYPE_ATTR = 'type';

  /**
   * @var string named schema name attribute name
   */
  const NAME_ATTR = 'name';

  /**
   * @var string named schema namespace attribute name
   */
  const NAMESPACE_ATTR = 'namespace';

  /**
   * @var string derived attribute: doesn't appear in schema
   */
  const FULLNAME_ATTR = 'fullname';

  /**
   * @var string array schema size attribute name
   */
  const SIZE_ATTR = 'size';

  /**
   * @var string record fields attribute name
   */
  const FIELDS_ATTR = 'fields';

  /**
   * @var string array schema items attribute name
   */
  const ITEMS_ATTR = 'items';

  /**
   * @var string enum schema symbols attribute name
   */
  const SYMBOLS_ATTR = 'symbols';

  /**
   * @var string map schema values attribute name
   */
  const VALUES_ATTR = 'values';

  /**
   * @var string document string attribute name
   */
  const DOC_ATTR = 'doc';

  /**
   * @var string the type of the current object of AvroSchema
   */
  protected $type;

  /**
   * @var array list of primitive schema type names
   */
  private static $primitiveTypes = [
    self::NULL_TYPE,
    self::BOOLEAN_TYPE,
    self::STRING_TYPE,
    self::BYTES_TYPE,
    self::INT_TYPE,
    self::LONG_TYPE,
    self::FLOAT_TYPE,
    self::DOUBLE_TYPE
  ];

  /**
   * @var array list of named schema type names
   */
  private static $namedTypes = [
    self::FIXED_SCHEMA,
    self::ENUM_SCHEMA,
    self::RECORD_SCHEMA,
    self::ERROR_SCHEMA
  ];

  /**
   * @param string $type a schema type name
   * @return boolean true if the given type name is a named schema type name
   *                 and false otherwise.
   */
  public static function isNamedType($type) {
    return in_array($type, self::$namedTypes);
  }

  /**
   * @param string $type a schema type name
   * @return boolean true if the given type name is a primitive schema type
   *                 name and false otherwise.
   */
  public static function isPrimitiveType($type) {
    return in_array($type, self::$primitiveTypes);
  }

  /**
   * @param string $type a schema type name
   * @return boolean true if the given type name is a valid schema type
   *                 name and false otherwise.
   */
  public static function isValidType($type) {
    return self::isPrimitiveType($type)
      || self::isNamedType($type)
      || in_array($type, array(self::ARRAY_SCHEMA,
        self::MAP_SCHEMA,
        self::UNION_SCHEMA,
        self::REQUEST_SCHEMA,
        self::ERROR_UNION_SCHEMA)
      );
  }

  /**
   * @var array list of names of reserved attributes
   */
  private static $reservedAttrs = [
    self::TYPE_ATTR,
    self::NAME_ATTR,
    self::NAMESPACE_ATTR,
    self::FIELDS_ATTR,
    self::ITEMS_ATTR,
    self::SIZE_ATTR,
    self::SYMBOLS_ATTR,
    self::VALUES_ATTR
  ];

  /**
   * @param string $attribute the attribute to be checked
   * @return boolean true if the given attribute is a reserved attribute
   */
  public static function isReservedAttribute($attribute) {
    return in_array($attribute, self::$reservedAttrs);
  }

  /**
   * @param string $json JSON-encoded schema
   * @uses self::realParse()
   * @return AvroSchema
   * @throws AvroSchemaParseException
   */
  public static function parse($json) {
    $schemata = new AvroNamedSchemata();
    return self::realParse(json_decode($json, true), null, $schemata);
  }

  /**
   * @param array|string $avro JSON-decoded schema string is for primitive types only
   * @param string|null $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata|null $schemata reference to named schemas
   * @return AvroSchema
   * @throws AvroSchemaParseException
   */
  static function realParse($avro, $defaultNamespace = null, AvroNamedSchemata $schemata = null) {
    if (is_array($avro)) {
      $type = AvroUtil::arrayValue($avro, self::TYPE_ATTR);

      if (self::isPrimitiveType($type)) {
        return new AvroPrimitiveSchema($type);
      } elseif (self::isNamedType($type)) {
        $name = AvroUtil::arrayValue($avro, self::NAME_ATTR);
        $namespace = AvroUtil::arrayValue($avro, self::NAMESPACE_ATTR);
        $newAvroName = new AvroName($name, $namespace, $defaultNamespace);
        $doc = AvroUtil::arrayValue($avro, self::DOC_ATTR);
        switch ($type) {
          case self::FIXED_SCHEMA:
            $size = AvroUtil::arrayValue($avro, self::SIZE_ATTR);
            return new AvroFixedSchema($newAvroName, $size, $schemata);
          case self::ENUM_SCHEMA:
            $symbols = AvroUtil::arrayValue($avro, self::SYMBOLS_ATTR);
            return new AvroEnumSchema($newAvroName, $doc, $symbols, $schemata);
          case self::RECORD_SCHEMA:
          case self::ERROR_SCHEMA:
            $fields = AvroUtil::arrayValue($avro, self::FIELDS_ATTR);
            return new AvroRecordSchema($newAvroName, $doc, $fields, $schemata, $type);
          default:
            throw new AvroSchemaParseException(sprintf('Unknown named type: %s', $type));
        }
      } elseif (self::isValidType($type)) {
        switch ($type) {
          case self::ARRAY_SCHEMA:
            return new AvroArraySchema($avro[self::ITEMS_ATTR], $defaultNamespace, $schemata);
          case self::MAP_SCHEMA:
            return new AvroMapSchema($avro[self::VALUES_ATTR], $defaultNamespace, $schemata);
          default:
            throw new AvroSchemaParseException(sprintf('Unknown valid type: %s', $type));
        }
      } elseif (!array_key_exists(self::TYPE_ATTR, $avro) && AvroUtil::isList($avro)) {
        return new AvroUnionSchema($avro, $defaultNamespace, $schemata);
      } else {
        throw new AvroSchemaParseException(sprintf('Undefined type: %s', $type));
      }
    } elseif (self::isPrimitiveType($avro)) {
      return new AvroPrimitiveSchema($avro);
    } else {
      throw new AvroSchemaParseException(
        sprintf('%s is not a schema we know about.', print_r($avro, true))
      );
    }
  }

  /**
   * @internal Should only be called from within the constructor of
   *           a class which extends AvroSchema
   * @param string $type a schema type name
   */
  protected function __construct($type) {
    $this->type = $type;
  }

  /**
   * @param mixed $avro
   * @param string $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata|null &$schemata
   * @return AvroSchema
   * @uses AvroSchema::real_parse()
   * @throws AvroSchemaParseException
   */
  protected static function subParse($avro, $defaultNamespace, AvroNamedSchemata $schemata = null) {
    try {
      return self::realParse($avro, $defaultNamespace, $schemata);
    } catch (AvroSchemaParseException $e) {
      throw $e;
    } catch (\Exception $e) {
      throw new AvroSchemaParseException(
        sprintf('Sub-schema is not a valid Avro schema. Bad schema: %s', print_r($avro, true))
      );
    }
  }

  /**
   * @return string schema type name of this schema
   * @deprecated
   */
  public function type() {
    return $this->type;
  }

  /**
   * @return string schema type name of this schema
   */
  public function getType() {
    return $this->type;
  }

  /**
   * @return mixed schema type which should be used for Avro definitions in most cases Avro complete definition is necessary
   */
  public function getSchemaName() {
    return $this->toAvro();
  }

  /**
   * @return mixed
   */
  public function toAvro() {
    return [
      self::TYPE_ATTR => $this->type
    ];
  }

  /**
   * @return string the JSON-encoded representation of this Avro schema.
   */
  public function __toString() {
    return json_encode($this->toAvro());
  }

  /**
   * Checks the data and write to the encoder based on type
   * @param mixed $datum the data to be written to the encoder
   * @param AvroIOBinaryEncoder $encoder the encoder to be used for
   * @throws AvroIOTypeException in case of unknown type
   * @throws AvroIOException in case of error while writing
   */
  public function write($datum, AvroIOBinaryEncoder $encoder) {
    if (!$this->isValidDatum($datum)) {
      throw new AvroIOTypeException($this, $datum);
    }
    $this->writeDatum($datum, $encoder);
  }

  /**
   * This function will be over written in the types which may contain string which should be passed on top
   * @return boolean true if the schema contains string and should be used for javaString type
   */
  public function hasString(): bool {
    return false;
  }

  /**
   * Checks to see if the $datum is valid for this kind of schema
   * @param mixed $datum the value to be checked
   * @return boolean true if the datum is valid for this schema type and false otherwise
   */
  public abstract function isValidDatum($datum);

  /**
   * Writes the datum to the encoder with the correct format
   * @param mixed $datum the datum which should be written on the encoder
   * @param AvroIOBinaryEncoder $encoder the encoder to be used
   * @throws AvroIOException if there was a problem while writing the datum to then encoder
   * @throws AvroIOTypeException in case of unknown type
   */
  public abstract function writeDatum($datum, AvroIOBinaryEncoder $encoder);

  /**
   * Deserialize JSON value to the corresponding object for this schema type
   * @param mixed $value the value in JSON value
   * @return mixed the result object corresponding to the schema type
   * @throws AvroException if the value is not possible for deserialization for this type
   */
  public abstract function deserializeJson($value);

  /**
   * Reads data from the decoder with the current format
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   * @param AvroSchema|null $readersSchema the local schema which may be different from remote schema which is being used to read the data
   *                        if null provided the same schema is used for both (reader/writer)
   * @return mixed the data read from the decoder based on current schema
   * @throws AvroException if the type is not known for this schema
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   * @throws AvroIOSchemaMatchException if the schema between reader and writer are not the same
   */
  public function read(AvroIOBinaryDecoder $decoder, AvroSchema $readersSchema = null) {
    if ($readersSchema === null) {
      return $this->readData($decoder, $this);
    }
    if ($this instanceof AvroUnionSchema || $readersSchema instanceof AvroUnionSchema) {
      // union case is special so we consider that they are compatible for now, and we will check in more detail while reading the data really
      return $this->readData($decoder, $this);
    }
    if (!$this->schemaMatches($readersSchema)) {
      throw new AvroIOSchemaMatchException($this, $readersSchema);
    }
    // Schema resolution: reader's schema is a union, writer's schema is not
    if ($readersSchema instanceof AvroUnionSchema && !($this instanceof AvroUnionSchema)) {
      foreach ($readersSchema->getSchemas() as $schema) {
        if ($this->schemaMatches($schema)) {
          return $this->read($decoder, $schema);
        }
      }
      throw new AvroIOSchemaMatchException($this, $readersSchema);
    }
    return $this->readData($decoder, $readersSchema);
  }

  /**
   * Checks to see if the the readersSchema is compatible with the current writersSchema ($this)
   * @param AvroSchema $readersSchema other schema to be checked with
   * @return boolean true if this schema is compatible with the readersSchema supplied
   */
  public abstract function schemaMatches(AvroSchema $readersSchema);

  /**
   * Reads data from the decoder with the current format
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   * @param AvroSchema $readersSchema the local schema which may be different from remote schema which is being used to read the data
   * @return mixed the data read from the decoder based on current schema
   * @throws AvroException if the type is not known for this schema
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   */
  public abstract function readData(AvroIOBinaryDecoder $decoder, AvroSchema $readersSchema);

  /**
   * Skips a data based on the current schema from the decoder
   *
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   *
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   * @throws AvroException in case of any error in the reading of data or conversion
   */
  public abstract function skipData(AvroIOBinaryDecoder $decoder);

  /**
   * Converts the $defaultValue to the corresponding format of the value needed for this schema
   *
   * @param mixed $defaultValue the value from which the defaultValue should be generated
   *
   * @return mixed the correct format of the value
   * @throws AvroException in case of any error in the reading of data or conversion
   */
  public abstract function readDefaultValue($defaultValue);

  /**
   * @param string $attribute the name of the attribute/function to be called
   * @return mixed value of the attribute with the given attribute name
   */
  public function attribute($attribute) {
    return $this->$attribute();
  }

}
