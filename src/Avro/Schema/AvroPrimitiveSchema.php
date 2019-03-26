<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:38
 */

namespace Avro\Schema;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
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
   * Checks to see if the the readersSchema is compatible with the current writersSchema ($this)
   * @param AvroSchema $readersSchema other schema to be checked with
   * @return boolean true if this schema is compatible with the readersSchema supplied
   */
  public function schemaMatches(AvroSchema $readersSchema) {
    $writersSchemaType = $this->getType();
    $readersSchemaType = $readersSchema->getType();
    if ($readersSchema instanceof AvroPrimitiveSchema) {
      // if both are primitive types we should check on more detail
      if ($readersSchemaType === $writersSchemaType) {
        // both are same type, great we are compatible
        return true;
      }
      if ($writersSchemaType === AvroSchema::INT_TYPE && in_array($readersSchemaType, [
          AvroSchema::LONG_TYPE,
          AvroSchema::FLOAT_TYPE,
          AvroSchema::DOUBLE_TYPE
        ])) {
        // writer was integer and reader can read it as the value is castable
        return true;
      }
      if ($writersSchemaType === AvroSchema::LONG_TYPE && in_array($readersSchemaType, [
          AvroSchema::FLOAT_TYPE,
          AvroSchema::DOUBLE_TYPE
        ])) {
        // writer was long integer and reader can read it
        return true;
      }
      if ($writersSchemaType === AvroSchema::FLOAT_TYPE && $readersSchemaType === AvroSchema::DOUBLE_TYPE) {
        // writer was float so it can be castable to double as double has more precision
        return true;
      }
    }
    // in any other case the readersSchema is not compatible with the current writersSchema
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
    switch ($this->getType()) {
      case AvroSchema::NULL_TYPE:
        return $decoder->readNull();
      case AvroSchema::BOOLEAN_TYPE:
        return $decoder->readBoolean();
      case AvroSchema::INT_TYPE:
        return $decoder->readInt();
      case AvroSchema::LONG_TYPE:
        return $decoder->readLong();
      case AvroSchema::FLOAT_TYPE:
        return $decoder->readFloat();
      case AvroSchema::DOUBLE_TYPE:
        return $decoder->readDouble();
      case AvroSchema::STRING_TYPE:
        return $decoder->readString();
      case AvroSchema::BYTES_TYPE:
        return $decoder->readBytes();
      default:
        // this is not possible as we are a primitive type
        throw new AvroException(sprintf("Cannot read unknown schema type: %s",$this->getType()));
    }
  }

  /**
   * Skips a data based on the current schema from the decoder
   *
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   * @throws AvroException in case there is another type which should not be possible for primitive types
   */
  public function skipData(AvroIOBinaryDecoder $decoder) {
    switch ($this->getType()) {
      case AvroSchema::NULL_TYPE:
        // nothing to be done
        break;
      case AvroSchema::BOOLEAN_TYPE:
        $decoder->skipBoolean();
        break;
      case AvroSchema::INT_TYPE:
        $decoder->skipInt();
        break;
      case AvroSchema::LONG_TYPE:
        $decoder->skipLong();
        break;
      case AvroSchema::FLOAT_TYPE:
        $decoder->skipFloat();
        break;
      case AvroSchema::DOUBLE_TYPE:
        $decoder->skipDouble();
        break;
      case AvroSchema::STRING_TYPE:
        $decoder->skipString();
        break;
      case AvroSchema::BYTES_TYPE:
        $decoder->skipBytes();
        break;
      default:
        // this is not possible as we are a primitive type
        throw new AvroException(sprintf("Cannot read unknown schema type: %s",$this->getType()));
    }
  }

  /**
   * Converts the $defaultValue to the corresponding format of the value needed for this schema
   *
   * @param mixed $defaultValue the value from which the defaultValue should be generated
   *
   * @return mixed the correct format of the value
   * @throws AvroException in case there is another type which should not be possible for primitive types
   */
  public function readDefaultValue($defaultValue) {
    switch ($this->getType()) {
      case AvroSchema::NULL_TYPE:
        return null;
      case AvroSchema::BOOLEAN_TYPE:
        return (boolean)$defaultValue;
      case AvroSchema::INT_TYPE:
      case AvroSchema::LONG_TYPE:
        return (int)$defaultValue;
      case AvroSchema::FLOAT_TYPE:
      case AvroSchema::DOUBLE_TYPE:
        return (float)$defaultValue;
      case AvroSchema::STRING_TYPE:
      case AvroSchema::BYTES_TYPE:
        return $defaultValue;
      default:
        // this is not possible as we are a primitive type
        throw new AvroException(sprintf("Cannot read unknown schema type: %s",$this->getType()));
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