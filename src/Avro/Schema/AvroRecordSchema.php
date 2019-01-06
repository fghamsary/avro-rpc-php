<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:13
 */

namespace Avro\Schema;

use Avro\AvroUtil;
use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Exception\AvroIOException;
use Avro\Record\AvroRecordHelper;
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
   * @param AvroNamedSchemata|null $schemata
   * @param string $schemaType schema type name
   * @throws AvroSchemaParseException
   */
  public function __construct(AvroName $name, $doc, $fields, AvroNamedSchemata $schemata = null, $schemaType = AvroSchema::RECORD_SCHEMA) {
    if ($fields === null) {
      throw new AvroSchemaParseException('Record schema requires a non-empty fields attribute');
    }
    if ($schemata === null) {
      $schemata = new AvroNamedSchemata();
    }
    if (AvroSchema::REQUEST_SCHEMA === $schemaType) {
      parent::__construct($schemaType, $name);
    } else {
      parent::__construct($schemaType, $name, $doc, $schemata);
    }
    $this->fields = self::parseFields($fields, $name->getNamespace(), $schemata);
  }

  /**
   * @param mixed $fieldData
   * @param string $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata|null $schemata
   * @return AvroField[] indexed by the name of the fields
   * @throws AvroSchemaParseException
   */
  static function parseFields($fieldData, $defaultNamespace, $schemata) {
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

      $isSchemaFromSchemata = false;
      $fieldSchema = null;
      if (is_string($type) &&
        $fieldSchema = $schemata->schemaByName(new AvroName($type, null, $defaultNamespace))) {
        $isSchemaFromSchemata = true;
      } else {
        $fieldSchema = self::subParse($type, $defaultNamespace, $schemata);
      }
      $fields[$name] = new AvroField($name, $fieldSchema, $isSchemaFromSchemata, $hasDefault, $default, $order);
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
        if (!array_key_exists($name, $datum) || !$field->getFieldType()->isValidDatum($datum[$name])) {
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
   * Checks to see if the the readersSchema is compatible with the current writersSchema ($this)
   * @param AvroSchema $readersSchema other schema to be checked with
   * @return boolean true if this schema is compatible with the readersSchema supplied
   */
  public function schemaMatches(AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroRecordSchema) {
      return $this->getFullname() === $readersSchema->getFullname();
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
    if ($readersSchema instanceof AvroRecordSchema) {
      $readersFields = $readersSchema->getFields();
      $record = AvroRecordHelper::getNewRecordInstance($readersSchema);
      $classBasedRecord = $record instanceof IAvroRecordBase;
      $setFieldValue = function ($name, $value) use (&$record, $classBasedRecord) {
        if ($classBasedRecord) {
          $record->_internalSetValue($name, $value);
        } else {
          $record[$name] = $value;
        }
      };
      foreach ($this->getFields() as $fieldName => $writersField) {
        $fieldType = $writersField->getFieldType();
        if (isset($readersFields[$fieldName])) {
          $value = $fieldType->read($decoder, $readersFields[$fieldName]->getFieldType());
          $setFieldValue($fieldName, $value);
        } else {
          $fieldType->skipData($decoder);
        }
      }
      // Fill in default values
      if (count($readersFields) > count($record)) {
        $writersFields = $this->getFields();
        foreach ($readersFields as $fieldName => $field) {
          if (!isset($writersFields[$fieldName])) {
            if ($field->hasDefaultValue()) {
              $value = $field->getFieldType()->readDefaultValue($field->getDefaultValue());
              $setFieldValue($fieldName, $value);
            } else {
              error_log("There is a problem in the Avro definition");
            }
          }
        }
      }
      return $record;
    } else {
      throw new AvroIOSchemaMatchException($this, $readersSchema);
    }
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
    foreach ($this->getFields() as $field) {
      $field->getFieldType()->skipData($decoder);
    }
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
    $fieldsList = $this->getFields();
    $record = AvroRecordHelper::getNewRecordInstance($this);
    $classBasedRecord = $record instanceof IAvroRecordBase;
    foreach ($fieldsList as $name => $field) {
      $fieldValue = AvroUtil::arrayValue($defaultValue, $name) ?: $field->getDefaultValue();
      $value = $field->getFieldType()->readDefaultValue($fieldValue);
      if ($classBasedRecord) {
        $record->_internalSetValue($name, $value);
      } else {
        $record[$name] = $value;
      }
    }
    return $record;
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $fieldsAvro = [];
    foreach ($this->getFields() as $field) {
      $fieldsAvro[] = $field->toAvro();
    }
    if (AvroSchema::REQUEST_SCHEMA == $this->getType()) {
      return $fieldsAvro;
    }
    $avro[AvroSchema::FIELDS_ATTR] = $fieldsAvro;
    return $avro;
  }

}
