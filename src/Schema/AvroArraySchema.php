<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:36
 */

namespace Avro\Schema;

use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOBinaryEncoder;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Exception\AvroIOException;

/**
 * Avro array schema, consisting of items of a particular
 * @package Avro\Schema
 */
class AvroArraySchema extends AvroSchema {

  /**
   * @var AvroNamedSchema|AvroSchema named schema name or AvroSchema of array element
   */
  private $itemsSchema;

  /**
   * @var boolean true if the items schema
   * FIXME: couldn't we derive this from whether or not $this->items
   *        is an AvroName or an AvroSchema?
   */
  private $is_items_schema_from_schemata;

  /**
   * @param string|mixed $items AvroNamedSchema name or object form
   *        of decoded JSON schema representation.
   * @param string $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata &$schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($items, $defaultNamespace, &$schemata = null) {
    parent::__construct(AvroSchema::ARRAY_SCHEMA);

    $this->is_items_schema_from_schemata = false;
    $itemsSchema = null;
    if (is_string($items) &&
      $itemsSchema = $schemata->schemaByName(new AvroName($items, null, $defaultNamespace))) {
      $this->is_items_schema_from_schemata = true;
    } else {
      $itemsSchema = AvroSchema::subParse($items, $defaultNamespace, $schemata);
    }
    $this->itemsSchema = $itemsSchema;
  }

  /**
   * @return AvroSchema AvroSchema or AvroNamedSchema of this array schema's elements.
   */
  public function getItemsSchema() {
    return $this->itemsSchema;
  }

  /**
   * The datum should be array and it should be of the type specified on the schema
   * @param array $datum the array which should be checked based on the current schema
   * @return bool true if all values on the datum are valid for this type
   */
  public function isValidDatum($datum) {
    if (is_array($datum)) {
      foreach ($datum as $d) {
        if (!$this->getItemsSchema()->isValidDatum($d)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Writes the array in the datum on the encoder
   * @param mixed $datum the value to be written
   * @param AvroIOBinaryEncoder $encoder the encoder which should be used for the write
   * @throws AvroIOException if there was a problem while writing the datum to then encoder
   * @throws AvroIOTypeException in case of unknown type
   */
  public function writeDatum($datum, AvroIOBinaryEncoder $encoder) {
    $datum_count = count($datum);
    if ($datum_count > 0) {
      $encoder->writeLong($datum_count);
      $itemSchema = $this->getItemsSchema();
      foreach ($datum as $item) {
        $itemSchema->writeDatum($item, $encoder);
      }
    }
    $encoder->writeLong(0);
  }

  /**
   * @returns array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::ITEMS_ATTR] = $this->is_items_schema_from_schemata ?
      $this->itemsSchema->getQualifiedName() :
      $this->itemsSchema->toAvro();
    return $avro;
  }
}
