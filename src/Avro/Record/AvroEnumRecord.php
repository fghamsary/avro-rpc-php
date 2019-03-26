<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:06
 */

namespace Avro\Record;

use Avro\Exception\AvroException;

/**
 * Base Enum class for avro enum schema implementation
 * Class AvroEnumRecord
 * @package Avro\Record
 */
abstract class AvroEnumRecord implements \JsonSerializable {
  public abstract static function _getSimpleAvroClassName();

  /** @var string */
  protected $value;

  protected function __construct($value) {
    $this->value = $value;
  }

  /**
   * @return string[] The list of available values for this enum
   */
  protected abstract static function getEnumValues();

  /**
   * @param string $value the value which should be checked
   * @return AvroEnumRecord the enum object corresponding to requested value
   * @throws AvroException if the value is not valid for this enum
   */
  public static function getItem($value) {
    if (static::hasValue($value)) {
      return new static($value);
    } else {
      throw new AvroException("$value is not valid for " . static::class . '!');
    }
  }

  /**
   * @param string $value The value of the enum to be checked
   * @return bool true if the value exists
   */
  public static function hasValue($value) {
    return in_array($value, static::getEnumValues());
  }

  /**
   * @return string the value of the enum
   */
  public function __toString() {
    return $this->value;
  }

  /**
   * @inheritDoc
   */
  public function jsonSerialize() {
    return $this->value;
  }
}
