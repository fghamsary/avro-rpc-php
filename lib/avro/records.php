<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 31/12/2018
 * Time: 09:53
 */

namespace Avro\Record;

use Avro\AvroRemoteException;

abstract class AvroRecord {
  public abstract static function getName();
}

abstract class AvroErrorRecord extends AvroRemoteException {
  public abstract static function getName();
}

abstract class AvroEnumRecord implements \JsonSerializable {
  public abstract static function getName();

  /** @var string */
  protected $value;

  protected function __construct($value) {
    $this->value = $value;
  }

  /**
   * @return string[] The list of available values for this enum
   */
  protected abstract static function getEnumValues();

  public static function getItem($value) {
    $class = get_called_class();
    if (self::hasValue($value)) {
      return new $class($value);
    } else {
      throw new \AvroException("$value is not valid for $class!");
    }
  }

  /**
   * @param string $value The value of the enum to be checked
   * @return bool true if the value exists
   */
  public static function hasValue($value) {
    return array_key_exists($value, self::getEnumValues());
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
