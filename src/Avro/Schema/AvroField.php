<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:17
 */

namespace Avro\Schema;

use Avro\Exception\AvroSchemaParseException;

/**
 * Class AvroField Field of an {@link AvroRecordSchema}
 * @package Avro\Schema
 */
class AvroField {

  /**
   * @var string fields name attribute name
   */
  const FIELD_NAME_ATTR = 'name';

  /**
   * @var string
   */
  const DEFAULT_ATTR = 'default';

  /**
   * @var string
   */
  const ORDER_ATTR = 'order';

  /**
   * @var string
   */
  const ASC_SORT_ORDER = 'ascending';

  /**
   * @var string
   */
  const DESC_SORT_ORDER = 'descending';

  /**
   * @var string
   */
  const IGNORE_SORT_ORDER = 'ignore';

  /**
   * @var array list of valid field sort order values
   */
  private static $valid_field_sort_orders = [
    self::ASC_SORT_ORDER,
    self::DESC_SORT_ORDER,
    self::IGNORE_SORT_ORDER
  ];

  /**
   * @param string $order
   * @return boolean
   */
  private static function isValidFieldSortOrder($order) {
    return in_array($order, self::$valid_field_sort_orders);
  }

  /**
   * @param string $order
   * @throws AvroSchemaParseException if $order is not a valid field order value.
   */
  private static function checkOderValue($order) {
    if ($order !== null && !self::isValidFieldSortOrder($order)) {
      throw new AvroSchemaParseException(sprintf('Invalid field sort order %s', $order));
    }
  }

  /**
   * @var string
   */
  private $name;

  /**
   * @var boolean whether or no there is a default value
   */
  private $hasDefault;

  /**
   * @var string field default value
   */
  private $default;

  /**
   * @var string sort order of this field
   */
  private $order;

  /**
   * @var AvroSchema|AvroNamedSchema the type for this field
   */
  private $fieldType;

  /**
   * @var boolean whether or not the AvroNamedSchema of this field is defined in the AvroNamedSchemata instance
   */
  private $is_type_from_schemata;

  /**
   * @param string $name
   * @param AvroSchema $schema
   * @param boolean $is_type_from_schemata
   * @param $has_default
   * @param string $default
   * @param string $order
   * @throws AvroSchemaParseException
   * @todo Check validity of $default value
   * @todo Check validity of $order value
   */
  public function __construct(string $name, $schema, $is_type_from_schemata,
                              $has_default, $default, $order = null) {
    if (!AvroName::isWellFormedName($name)) {
      throw new AvroSchemaParseException('Field requires a "name" attribute');
    }
    $this->fieldType = $schema;
    $this->is_type_from_schemata = $is_type_from_schemata;
    $this->name = $name;
    $this->hasDefault = $has_default;
    if ($this->hasDefault) {
      $this->default = $default;
    }
    $this->checkOderValue($order);
    $this->order = $order;
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = [
      AvroField::FIELD_NAME_ATTR => $this->name,
      AvroSchema::TYPE_ATTR => $this->is_type_from_schemata ? $this->fieldType->getQualifiedName() : $this->fieldType->toAvro()
    ];
    if (isset($this->default)) {
      $avro[AvroField::DEFAULT_ATTR] = $this->default;
    }
    if ($this->order) {
      $avro[AvroField::ORDER_ATTR] = $this->order;
    }
    return $avro;
  }

  /**
   * @return AvroSchema the type for this field
   */
  public function getFieldType() {
    return $this->fieldType;
  }

  /**
   * @return string the name of this field
   */
  public function getName() {
    return $this->name;
  }

  /**
   * @return mixed the default value of this field
   */
  public function getDefaultValue() {
    return $this->default;
  }

  /**
   * @return boolean true if the field has a default and false otherwise
   */
  public function hasDefaultValue() {
    return $this->hasDefault;
  }
}
