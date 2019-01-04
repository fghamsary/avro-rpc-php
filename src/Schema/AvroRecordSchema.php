<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:13
 */

namespace Avro\Schema;

use Avro\AvroUtil;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOBinaryEncoder;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Exception\AvroIOException;
use Avro\Record\IAvroRecordBase;

/**
 * Class AvroRecordSchema the type for the records of Avro
 * @package Avro\Schema
 */
class AvroRecordSchema extends AvroNamedSchema {

  /**
   * @var AvroSchema[] array of AvroSchema fields indexed by name of fields
   */
  private $fields;

  /**
   * @param AvroName $name
   * @param string $doc
   * @param array $fields
   * @param AvroNamedSchemata &$schemata
   * @param string $schema_type schema type name
   * @throws AvroSchemaParseException
   */
  public function __construct(AvroName $name, $doc, $fields, &$schemata = null, $schema_type = AvroSchema::RECORD_SCHEMA) {
    if ($fields === null) {
      throw new AvroSchemaParseException('Record schema requires a non-empty fields attribute');
    }
    if (AvroSchema::REQUEST_SCHEMA == $schema_type) {
      parent::__construct($schema_type, $name);
    } else {
      parent::__construct($schema_type, $name, $doc, $schemata);
    }
    $this->fields = self::parseFields($fields, $name->getNamespace(), $schemata);
  }

  /**
   * @param mixed $fieldData
   * @param string $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata &$schemata
   * @return AvroField[] indexed by the name of the fields
   * @throws AvroSchemaParseException
   */
  static function parseFields($fieldData, $defaultNamespace, &$schemata) {
    $fields = [];
    foreach ($fieldData as $index => $field) {
      $name = AvroUtil::arrayValue($field, AvroField::FIELD_NAME_ATTR);
      $type = AvroUtil::arrayValue($field, AvroSchema::TYPE_ATTR);
      $order = AvroUtil::arrayValue($field, AvroField::ORDER_ATTR);

      $default = null;
      $hasDefault = false;
      if (array_key_exists(AvroField::DEFAULT_ATTR, $field)) {
        $default = $field[AvroField::DEFAULT_ATTR];
        $hasDefault = true;
      }

      if (array_key_exists($name, $fields)) {
        throw new AvroSchemaParseException(sprintf("Field name %s is already in use", $name));
      }

      $is_schema_from_schemata = false;
      $fieldSchema = null;
      if (is_string($type) &&
        $fieldSchema = $schemata->schemaByName(new AvroName($type, null, $defaultNamespace))) {
        $is_schema_from_schemata = true;
      } else {
        $fieldSchema = self::subParse($type, $defaultNamespace, $schemata);
      }
      $fields[$name] = new AvroField($name, $fieldSchema, $is_schema_from_schemata, $hasDefault, $default, $order);
    }
    return $fields;
  }

  /**
   * @return AvroField[] array the schema definitions of the fields of this AvroRecordSchema indexed by field name
   */
  public function getFields() {
    return $this->fields;
  }

  /**
   * Checks to see if the datum is of type IAvroRecordBase defined for this record and if not an array (map) with
   *  correct fields
   * @param mixed $datum
   * @return bool
   */
  public function isValidDatum($datum) {
    if ($datum instanceof IAvroRecordBase) {
      return $datum->_getSimpleAvroClassName() === $this->getQualifiedName();
    }
    if (is_array($datum)) {
      foreach ($this->getFields() as $name => $field) {
        if (!array_key_exists($name, $datum) || $field->getFieldType()->isValidDatum($datum[$name])) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Writes the datum record based on the schema to the encoder
   * @param mixed $datum the value to be written
   * @param AvroIOBinaryEncoder $encoder the encoder which should be used for the write
   * @throws AvroIOException if there was a problem while writing the datum to then encoder
   * @throws AvroIOTypeException in case of unknown type
   */
  public function writeDatum($datum, AvroIOBinaryEncoder $encoder) {
    foreach ($this->getFields() as $name => $field) {
      if ($datum instanceof IAvroRecordBase) {
        $field->getFieldType()->writeDatum($datum->_internalGetValue($name), $encoder);
      } else {
        $field->getFieldType()->writeDatum($datum[$name], $encoder);
      }
    }
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $fieldsAvro = [];
    foreach ($this->fields as $field)
      $fieldsAvro[] = $field->toAvro();
    if (AvroSchema::REQUEST_SCHEMA == $this->type) {
      return $fieldsAvro;
    }
    $avro[AvroSchema::FIELDS_ATTR] = $fieldsAvro;
    return $avro;
  }

}
