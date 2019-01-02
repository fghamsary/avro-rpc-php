<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 31/12/2018
 * Time: 09:53
 */

namespace Avro\Record;

use Avro\AvroException;
use Avro\AvroRemoteException;

interface IAvroRecordBase extends \Countable {
  /**
   * @return string Get the name of the current record type
   */
  public static function getName();

  /**
   * @return $this Instances new record based on the type selected
   */
  public static function newInstance();

  /**
   * @param string $field The field which we want the value for
   * @return mixed
   */
  public function _internalGetValue($field);

  /**
   * @param string $field The field which should be set
   * @param mixed $value The value which should be set
   * @return $this
   */
  public function _internalSetValue($field, $value);
}

trait TAvroRecordBase {

  private $iFieldsSet = 0;

  public function _internalGetValue($field) {
    $fieldGetter = 'get' . ucfirst($field);
    return $this->$fieldGetter();
  }

  public function _internalSetValue($field, $value) {
    $this->iFieldsSet++;
    $fieldSetter = 'set' . ucfirst($field);
    return $this->$fieldSetter($value);
  }

  /**
   * @return int the number of fields that have been set via _internalSetValue
   */
  public function count() {
    return $this->iFieldsSet;
  }

}

abstract class AvroRecord implements IAvroRecordBase {
  use TAvroRecordBase;
  public abstract static function getName();

  /**
   * @return static Creates new instance of the current record type
   */
  public static function newInstance() {
    return new static();
  }

}

abstract class AvroErrorRecord extends AvroRemoteException implements IAvroRecordBase {
  use TAvroRecordBase;
  public abstract static function getName();

  /**
   * @return static Creates new instance of the current error type
   */
  public static function newInstance() {
    return new static();
  }
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
    if (self::hasValue($value)) {
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
