<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 13:36
 */

namespace Avro\Schema;

use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\AvroIOTypeException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
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
  private $alreadyInSchemata;

  /**
   * @param string|mixed $items AvroNamedSchema name or object form
   *        of decoded JSON schema representation.
   * @param string $defaultNamespace namespace of enclosing schema
   * @param AvroNamedSchemata|null $schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($items, $defaultNamespace, $schemata = null) {
    parent::__construct(AvroSchema::ARRAY_SCHEMA);
    if ($schemata === null) {
      $schemata = new AvroNamedSchemata();
    }
    $this->alreadyInSchemata = false;
    $itemsSchema = null;
    if (is_string($items) &&
      $itemsSchema = $schemata->getSchemaByName(new AvroName($items, null, $defaultNamespace))) {
      $this->alreadyInSchemata = true;
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
   * Checks to see if the the readersSchema is compatible with the current writersSchema ($this)
   * @param AvroSchema $readersSchema other schema to be checked with
   * @return boolean true if this schema is compatible with the readersSchema supplied
   */
  public function schemaMatches(AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroArraySchema) {
      return $this->getItemsSchema()->schemaMatches($readersSchema->getItemsSchema());
    }
    return false;
  }

  /**
   * Reads array data from the decoder with the current format
   * @param AvroIOBinaryDecoder $decoder the decoder to be used
   * @param AvroSchema $readersSchema the local schema which may be different from remote schema which is being used to read the data
   * @return array the data read from the decoder based on current schema
   * @throws AvroException if the type is not known for this schema
   * @throws AvroIOException thrown if there was a problem while reading the data from decoder
   * @throws AvroIOSchemaMatchException if the schema between reader and writer are not the same
   */
  public function readData(AvroIOBinaryDecoder $decoder, AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroArraySchema) {
      $items = [];
      $blockCount = $decoder->readLong();
      $writersItemsSchema = $this->getItemsSchema();
      $readersItemSchema = $readersSchema->getItemsSchema();
      while ($blockCount !== 0) {
        if ($blockCount < 0) {
          $blockCount = -$blockCount;
          $decoder->skipLong(); // Read (and ignore) block size
        }
        for ($i = 0; $i < $blockCount; $i++) {
          $items[] = $writersItemsSchema->read($decoder, $readersItemSchema);
        }
        $blockCount = $decoder->readLong();
      }
      return $items;
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
    $blockCount = $decoder->readLong();
    $itemSchema = $this->getItemsSchema();
    while ($blockCount !== 0) {
      if ($blockCount < 0) {
        $blockCount = -$blockCount;
        $decoder->skipLong(); // Read (and ignore) block size
      }
      for ($i = 0; $i < $blockCount; $i++) {
        $itemSchema->skipData($decoder);
      }
      $blockCount = $decoder->readLong();
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
    $result = [];
    $itemSchema = $this->getItemsSchema();
    foreach ($defaultValue as $jsonVal) {
      $result[] = $itemSchema->readDefaultValue($jsonVal);
    }
    return $result;
  }

  /**
   * @returns array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::ITEMS_ATTR] = $this->alreadyInSchemata ?
      $this->itemsSchema->getQualifiedName() :
      $this->itemsSchema->toAvro();
    return $avro;
  }
}
