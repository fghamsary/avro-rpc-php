<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 14:03
 */

namespace Avro\IO;

use Avro\Exception\AvroException;
use Avro\Schema\AvroSchema;

/**
 * Class AvroIODatumReader
 *
 * Handles schema-specif reading of data from the decoder.
 *
 * Also handles schema resolution between the reader and writer schemas (if a writer's schema is provided).
 *
 * @package Avro\IO
 */
class AvroIODatumReader {

  /**
   *
   * @param AvroSchema $writersSchema
   * @param AvroSchema $readersSchema
   * @return boolean true if the schemas are consistent with each other and false otherwise.
   */
  static function schemas_match(AvroSchema $writersSchema, AvroSchema $readersSchema) {
    $writersSchemaType = $writersSchema->getType();
    $readersSchemaType = $readersSchema->getType();

    if (AvroSchema::UNION_SCHEMA == $writersSchemaType || AvroSchema::UNION_SCHEMA == $readersSchemaType) {
      return true;
    }

    if ($writersSchemaType == $readersSchemaType) {
      if (AvroSchema::isPrimitiveType($writersSchemaType)) {
        return true;
      }

      switch ($readersSchemaType) {
        case AvroSchema::MAP_SCHEMA:
          return self::attributes_match($writersSchema->values(),
            $readersSchema->values(),
            array(AvroSchema::TYPE_ATTR));
        case AvroSchema::ARRAY_SCHEMA:
          return self::attributes_match($writersSchema->items(),
            $readersSchema->items(),
            array(AvroSchema::TYPE_ATTR));
        case AvroSchema::ENUM_SCHEMA:
          return self::attributes_match($writersSchema, $readersSchema,
            array(AvroSchema::FULLNAME_ATTR));
        case AvroSchema::FIXED_SCHEMA:
          return self::attributes_match($writersSchema, $readersSchema,
            array(AvroSchema::FULLNAME_ATTR,
              AvroSchema::SIZE_ATTR));
        case AvroSchema::RECORD_SCHEMA:
        case AvroSchema::ERROR_SCHEMA:
          return self::attributes_match($writersSchema, $readersSchema,
            array(AvroSchema::FULLNAME_ATTR));
        case AvroSchema::REQUEST_SCHEMA:
          // XXX: This seems wrong
          return true;
        // XXX: no default
      }

      if (AvroSchema::INT_TYPE == $writersSchemaType &&
        in_array($readersSchemaType, array(AvroSchema::LONG_TYPE,
          AvroSchema::FLOAT_TYPE,
          AvroSchema::DOUBLE_TYPE))) {
        return true;
      }

      if (AvroSchema::LONG_TYPE == $writersSchemaType &&
        in_array($readersSchemaType, array(AvroSchema::FLOAT_TYPE, AvroSchema::DOUBLE_TYPE))) {
        return true;
      }

      if (AvroSchema::FLOAT_TYPE == $writersSchemaType && AvroSchema::DOUBLE_TYPE == $readersSchemaType) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks equivalence of the given attributes of the two given schemas.
   *
   * @param AvroSchema $schema_one
   * @param AvroSchema $schema_two
   * @param string[] $attribute_names array of string attribute names to compare
   *
   * @return boolean true if the attributes match and false otherwise.
   */
  static function attributes_match($schema_one, $schema_two, $attribute_names) {
    foreach ($attribute_names as $attribute_name) {
      if ($schema_one->attribute($attribute_name) != $schema_two->attribute($attribute_name)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @var AvroSchema
   */
  private $writers_schema;

  /**
   * @var AvroSchema
   */
  private $readers_schema;

  /**
   * @var string The default namespace which should be used to search for classes
   */
  private $default_namespace = '\\';

  /**
   * @param AvroSchema $writers_schema
   * @param AvroSchema $readers_schema
   */
  function __construct($writers_schema = null, $readers_schema = null) {
    $this->writers_schema = $writers_schema;
    $this->readers_schema = $readers_schema;
  }

  public function set_default_namespace($default_namespace) {
    $this->default_namespace = $default_namespace;
  }

  /**
   * @param AvroSchema $readers_schema
   */
  public function set_writers_schema($readers_schema) {
    $this->writers_schema = $readers_schema;
  }

  /**
   * @param AvroIOBinaryDecoder $decoder
   * @return string
   * @throws AvroException
   * @throws AvroIOSchemaMatchException
   */
  public function read($decoder) {
    if ($this->readers_schema === null) {
      $this->readers_schema = $this->writers_schema;
    }
    return $this->read_data($this->writers_schema, $this->readers_schema, $decoder);
  }

  /**#
   * @param AvroSchema $writers_schema
   * @param AvroSchema $readers_schema
   * @param AvroIOBinaryDecoder $decoder
   * @return mixed
   * @throws AvroException
   * @throws AvroIOSchemaMatchException
   */
  public function read_data(AvroSchema $writers_schema, AvroSchema $readers_schema, AvroIOBinaryDecoder $decoder) {
    if (!self::schemas_match($writers_schema, $readers_schema)) {
      throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
    }

    // Schema resolution: reader's schema is a union, writer's schema is not
    if (AvroSchema::UNION_SCHEMA == $readers_schema->type() && AvroSchema::UNION_SCHEMA != $writers_schema->type()) {
      foreach ($readers_schema->schemas() as $schema) {
        if (self::schemas_match($writers_schema, $schema)) {
          return $this->read_data($writers_schema, $schema, $decoder);
        }
      }
      throw new AvroIOSchemaMatchException($writers_schema, $readers_schema);
    }

    switch ($writers_schema->type()) {
      case AvroSchema::NULL_TYPE:
        return $decoder->read_null();
      case AvroSchema::BOOLEAN_TYPE:
        return $decoder->read_boolean();
      case AvroSchema::INT_TYPE:
        return $decoder->read_int();
      case AvroSchema::LONG_TYPE:
        return $decoder->read_long();
      case AvroSchema::FLOAT_TYPE:
        return $decoder->read_float();
      case AvroSchema::DOUBLE_TYPE:
        return $decoder->read_double();
      case AvroSchema::STRING_TYPE:
        return $decoder->read_string();
      case AvroSchema::BYTES_TYPE:
        return $decoder->read_bytes();
      case AvroSchema::ARRAY_SCHEMA:
        return $this->read_array($writers_schema, $readers_schema, $decoder);
      case AvroSchema::MAP_SCHEMA:
        return $this->read_map($writers_schema, $readers_schema, $decoder);
      case AvroSchema::UNION_SCHEMA:
        return $this->read_union($writers_schema, $readers_schema, $decoder);
      case AvroSchema::ENUM_SCHEMA:
        return $this->read_enum($writers_schema, $readers_schema, $decoder);
      case AvroSchema::FIXED_SCHEMA:
        return $this->read_fixed($writers_schema, $readers_schema, $decoder);
      case AvroSchema::RECORD_SCHEMA:
      case AvroSchema::ERROR_SCHEMA:
      case AvroSchema::REQUEST_SCHEMA:
        return $this->read_record($writers_schema, $readers_schema, $decoder);
      default:
        throw new AvroException(sprintf("Cannot read unknown schema type: %s",
          $writers_schema->type()));
    }
  }

  /**
   * @returns array
   */
  public function read_array($writers_schema, $readers_schema, $decoder) {
    $items = array();
    $block_count = $decoder->read_long();
    while (0 != $block_count) {
      if ($block_count < 0) {
        $block_count = -$block_count;
        $block_size = $decoder->read_long(); // Read (and ignore) block size
      }
      for ($i = 0; $i < $block_count; $i++)
        $items [] = $this->read_data($writers_schema->items(),
          $readers_schema->items(),
          $decoder);
      $block_count = $decoder->read_long();
    }
    return $items;
  }

  /**
   * @returns array
   */
  public function read_map($writers_schema, $readers_schema, $decoder) {
    $items = array();
    $pair_count = $decoder->read_long();
    while (0 != $pair_count) {
      if ($pair_count < 0) {
        $pair_count = -$pair_count;
        // Note: we're not doing anything with block_size other than skipping it
        $block_size = $decoder->read_long();
      }

      for ($i = 0; $i < $pair_count; $i++) {
        $key = $decoder->read_string();
        $items[$key] = $this->read_data($writers_schema->values(),
          $readers_schema->values(),
          $decoder);
      }
      $pair_count = $decoder->read_long();
    }
    return $items;
  }

  /**
   * @returns mixed
   */
  public function read_union($writers_schema, $readers_schema, $decoder) {
    $schema_index = $decoder->read_long();
    $selected_writers_schema = $writers_schema->schema_by_index($schema_index);
    return $this->read_data($selected_writers_schema, $readers_schema, $decoder);
  }

  /**
   * @returns string
   */
  public function read_enum($writers_schema, $readers_schema, $decoder) {
    $symbol_index = $decoder->read_int();
    $symbol = $writers_schema->symbol_by_index($symbol_index);
    $enumName = $this->default_namespace . $writers_schema->qualified_name();
    if (class_exists($enumName)) {
      /** @var AvroEnumRecord $enumName */
      $symbol = $enumName::getItem($symbol);
    }
    return $symbol;
  }

  /**
   * @returns string
   */
  public function read_fixed($writers_schema, $readers_schema, $decoder) {
    return $decoder->read($writers_schema->size());
  }

  /**
   * @returns array
   */
  public function read_record($writers_schema, $readers_schema, $decoder) {
    $readers_fields = $readers_schema->fields_hash();
    $recordClassName = $this->default_namespace . $readers_schema->qualified_name();
    $classBasedRecord = false;
    if (class_exists($recordClassName)) {
      /** @var IAvroRecordBase $record */
      $record = $recordClassName::newInstance();
      $classBasedRecord = true;
    } else {
      $record = array();
    }
    foreach ($writers_schema->fields() as $writers_field) {
      $type = $writers_field->type();
      if (isset($readers_fields[$writers_field->name()])) {
        $value = $this->read_data($type, $readers_fields[$writers_field->name()]->type(), $decoder);
        if ($classBasedRecord) {
          $record->_internalSetValue($writers_field->name(), $value);
        } else {
          $record[$writers_field->name()] = $value;
        }
      } else {
        $this->skip_data($type, $decoder);
      }
    }
    // Fill in default values
    if (count($readers_fields) > count($record)) {
      $writers_fields = $writers_schema->fields_hash();
      foreach ($readers_fields as $field_name => $field) {
        if (!isset($writers_fields[$field_name])) {
          if ($field->has_default_value()) {
            $value = $this->read_default_value($field->type(), $field->default_value());
            if ($classBasedRecord) {
              $record->_internalSetValue($field->name(), $value);
            } else {
              $record[$field->name()] = $value;
            }
          } else {
            error_log("There is a problem in the Avro definition");
          }
        }
      }
    }

    return $record;
  }
  /**#@-*/

  /**
   * @param AvroSchema $field_schema
   * @param null|boolean|int|float|string|array $default_value
   * @return null|boolean|int|float|string|array
   *
   * @throws AvroException if $field_schema type is unknown.
   */
  public function read_default_value($field_schema, $default_value) {
    switch ($field_schema->type()) {
      case AvroSchema::NULL_TYPE:
        return null;
      case AvroSchema::BOOLEAN_TYPE:
        return $default_value;
      case AvroSchema::INT_TYPE:
      case AvroSchema::LONG_TYPE:
        return (int)$default_value;
      case AvroSchema::FLOAT_TYPE:
      case AvroSchema::DOUBLE_TYPE:
        return (float)$default_value;
      case AvroSchema::STRING_TYPE:
      case AvroSchema::BYTES_TYPE:
        return $default_value;
      case AvroSchema::ARRAY_SCHEMA:
        $array = array();
        foreach ($default_value as $json_val) {
          $val = $this->read_default_value($field_schema->items(), $json_val);
          $array [] = $val;
        }
        return $array;
      case AvroSchema::MAP_SCHEMA:
        $map = array();
        foreach ($default_value as $key => $json_val)
          $map[$key] = $this->read_default_value($field_schema->values(),
            $json_val);
        return $map;
      case AvroSchema::UNION_SCHEMA:
        return $this->read_default_value($field_schema->schema_by_index(0),
          $default_value);
      case AvroSchema::ENUM_SCHEMA:
      case AvroSchema::FIXED_SCHEMA:
        return $default_value;
      case AvroSchema::RECORD_SCHEMA:
        $classBasedRecord = false;
        $recordClassName = $field_schema->qualified_name();
        if (class_exists($recordClassName)) {
          /** @var IAvroRecordBase $record */
          $record = $recordClassName::newInstance();
          $classBasedRecord = true;
        } else {
          $record = array();
        }
        foreach ($field_schema->fields() as $field) {
          $field_name = $field->name();
          if (!$json_val = $default_value[$field_name]) {
            $json_val = $field->default_value();
          }
          $value = $this->read_default_value($field->type(), $json_val);
          if ($classBasedRecord) {
            $record->_internalSetValue($field_name, $value);
          } else {
            $record[$field_name] = $value;
          }
        }
        return $record;
      default:
        throw new AvroException(sprintf('Unknown type: %s', $field_schema->type()));
    }
  }

  /**
   * @param AvroSchema $writers_schema
   * @param AvroIOBinaryDecoder $decoder
   */
  private function skip_data($writers_schema, $decoder) {
    switch ($writers_schema->type()) {
      case AvroSchema::NULL_TYPE:
        return $decoder->skip_null();
      case AvroSchema::BOOLEAN_TYPE:
        return $decoder->skip_boolean();
      case AvroSchema::INT_TYPE:
        return $decoder->skip_int();
      case AvroSchema::LONG_TYPE:
        return $decoder->skip_long();
      case AvroSchema::FLOAT_TYPE:
        return $decoder->skip_float();
      case AvroSchema::DOUBLE_TYPE:
        return $decoder->skip_double();
      case AvroSchema::STRING_TYPE:
        return $decoder->skip_string();
      case AvroSchema::BYTES_TYPE:
        return $decoder->skip_bytes();
      case AvroSchema::ARRAY_SCHEMA:
        return $decoder->skip_array($writers_schema, $decoder);
      case AvroSchema::MAP_SCHEMA:
        return $decoder->skip_map($writers_schema, $decoder);
      case AvroSchema::UNION_SCHEMA:
        return $decoder->skip_union($writers_schema, $decoder);
      case AvroSchema::ENUM_SCHEMA:
        return $decoder->skip_enum($writers_schema, $decoder);
      case AvroSchema::FIXED_SCHEMA:
        return $decoder->skip_fixed($writers_schema, $decoder);
      case AvroSchema::RECORD_SCHEMA:
      case AvroSchema::ERROR_SCHEMA:
      case AvroSchema::REQUEST_SCHEMA:
        return $decoder->skip_record($writers_schema, $decoder);
      default:
        throw new AvroException(sprintf('Unknown schema type: %s',
          $writers_schema->type()));
    }
  }
}
