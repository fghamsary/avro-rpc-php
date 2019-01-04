<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:38
 */

namespace Avro\Schema;

use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOBinaryEncoder;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Exception\AvroIOException;

/**
 * Avro schema for basic types such as null, int, long, string.
 * Class AvroPrimitiveSchema
 * @package Avro\Schema
 */
class AvroPrimitiveSchema extends AvroSchema {

  private static $JAVA_STRING_TYPE = false;

  /**
   * @param boolean $enable if true the strings are treated as java string type
   */
  public static function setJavaStringType($enable) {
    self::$JAVA_STRING_TYPE = $enable;
  }

  /**
   * @return bool if true we should treat strings as java type
   */
  public static function isJavaStringType() {
    return self::$JAVA_STRING_TYPE;
  }

  /**
   * @param string $type the primitive schema type name
   * @throws AvroSchemaParseException if the given $type is not a primitive schema type name
   */
  public function __construct($type) {
    if (!self::isPrimitiveType($type)) {
      throw new AvroSchemaParseException(sprintf('%s is not a valid primitive type.', $type));
    }
    parent::__construct($type);
  }

  /**
   * @return boolean true if this type is a string
   */
  public function isString() {
    return $this->getType() === self::STRING_TYPE;
  }

  /**
   * This should be a primitive value corresponding to the type specified
   * @param mixed $datum the value to be checked
   * @return bool true if the datum is valid
   */
  public function isValidDatum($datum) {
    switch ($this->getType()) {
      case self::NULL_TYPE:
        return $datum === null;
      case self::BOOLEAN_TYPE:
        return is_bool($datum);
      case self::STRING_TYPE:
      case self::BYTES_TYPE:
        return is_string($datum);
      case self::INT_TYPE:
        return (is_int($datum)
          && (self::INT_MIN_VALUE <= $datum)
          && ($datum <= self::INT_MAX_VALUE));
      case self::LONG_TYPE:
        return (is_int($datum)
          && (self::LONG_MIN_VALUE <= $datum)
          && ($datum <= self::LONG_MAX_VALUE));
      case self::FLOAT_TYPE:
      case self::DOUBLE_TYPE:
        return (is_float($datum) || is_int($datum));
    }
    return false;
  }

  /**
   * Writes primitive data to the encoder with the correct format
   * @param mixed $datum the value to be written
   * @param AvroIOBinaryEncoder $encoder the encoder which should be used for the write
   * @throws AvroIOException if there was a problem while writing the datum to then encoder
   * @throws AvroIOTypeException in case of unknown type
   */
  public function writeDatum($datum, AvroIOBinaryEncoder $encoder) {
    switch ($this->getType()) {
      case AvroSchema::NULL_TYPE:
        // there is nothing to be written
        break;
      case AvroSchema::BOOLEAN_TYPE:
        $encoder->writeBoolean($datum);
        break;
      case AvroSchema::INT_TYPE:
        $encoder->writeInt($datum);
        break;
      case AvroSchema::LONG_TYPE:
        $encoder->writeLong($datum);
        break;
      case AvroSchema::FLOAT_TYPE:
        $encoder->writeFloat($datum);
        break;
      case AvroSchema::DOUBLE_TYPE:
        $encoder->writeDouble($datum);
        break;
      case AvroSchema::STRING_TYPE:
        $encoder->writeString($datum);
        break;
      case AvroSchema::BYTES_TYPE:
        $encoder->writeBytes($datum);
        break;
      default:
        throw new AvroIOTypeException($this, $datum);
    }
  }

  /**
   * @return string|array
   */
  public function toAvro() {
    // only case of string we may need the avro.java.string attribute as well
    if (self::isJavaStringType() && $this->isString()) {
      return [
        self::TYPE_ATTR => self::STRING_TYPE,
        self::JAVA_STRING_ANNOTATION => self::JAVA_STRING_TYPE
      ];
    }
    return $this->getType();
  }
}