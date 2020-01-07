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
  public abstract static function _getSimpleAvroClassName(): string;

  /** @var string */
  protected $value;

  protected function __construct(string $value) {
    $this->value = $value;
  }

  /**
   * @return AvroEnumRecord[] of all instances possible hashed by their values as key for this enum
   */
  protected abstract static function getEnumValues(): array;

  /**
   * @param string $value the value which should be checked
   * @return AvroEnumRecord the enum object corresponding to requested value
   * @throws AvroException if the value is not valid for this enum
   */
  public static function getItem(string $value) {
    $values = static::getEnumValues();
    if (array_key_exists($value, $values)) {
      return $values[$value];
    } else {
      throw new AvroException("$value is not valid for " . static::class . '!');
    }
  }

  /**
   * @param string $value The value of the enum to be checked
   * @return bool true if the value exists
   */
  public static function hasValue(string $value) {
    return array_key_exists($value, static::getEnumValues());
  }

  /**
   * @return string the value of the enum
   */
  public function __toString(): string {
    return $this->value;
  }

  /**
   * @inheritDoc
   */
  public function jsonSerialize() {
    return $this->value;
  }

  /**
   * @param AvroEnumRecord|null $that the other value to be checked if it has the same value
   * @return bool true if the value is the same
   */
  public function equals(?AvroEnumRecord $that): bool {
    if ($that !== null) {
      return $this::_getSimpleAvroClassName() === $this::_getSimpleAvroClassName() &&
        $this->value === $that->value;
    }
    return false;
  }
}
