<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:58
 */

namespace Avro\Schema;

use Avro\AvroUtil;
use Avro\Exception\AvroException;
use Avro\Exception\AvroSchemaParseException;
use Avro\IO\AvroIOSchemaMatchException;
use Avro\IO\Binary\AvroIOBinaryDecoder;
use Avro\IO\Binary\AvroIOBinaryEncoder;
use Avro\IO\Exception\AvroIOException;
use Avro\Record\AvroEnumRecord;
use Avro\Record\AvroRecordHelper;

/**
 * Class AvroEnumSchema enum schema representation
 * @package Avro\Schema
 */
class AvroEnumSchema extends AvroNamedSchema {
  
  /**
   * @var string[] array of symbols
   */
  private $symbols;

  /**
   * @param AvroName $name
   * @param string $doc
   * @param string[] $symbols
   * @param AvroNamedSchemata|null $schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($name, $doc, $symbols, AvroNamedSchemata $schemata = null) {
    if (!AvroUtil::isList($symbols)) {
      throw new AvroSchemaParseException('Enum Schema symbols are not a list');
    }

    if (count(array_unique($symbols)) > count($symbols)) {
      throw new AvroSchemaParseException(sprintf('Duplicate symbols: %s', $symbols));
    }

    foreach ($symbols as $symbol) {
      if (!is_string($symbol) || empty($symbol)) {
        throw new AvroSchemaParseException(
          sprintf('Enum schema symbol must be a string %', print_r($symbol, true))
        );
      }
    }

    parent::__construct(AvroSchema::ENUM_SCHEMA, $name, $doc, $schemata);
    $this->symbols = $symbols;
  }

  /**
   * @return string[] this enum schema's symbols
   */
  public function getSymbols() {
    return $this->symbols;
  }

  /**
   * @param string $symbol
   * @return boolean true if the given symbol exists in this
   *         enum schema and false otherwise
   */
  public function hasSymbol($symbol) {
    return in_array($symbol, $this->symbols);
  }

  /**
   * @param int $index
   * @return string enum schema symbol with the given (zero-based) index
   * @throws AvroException in case the $index doesn't exist on the symbols defined
   */
  public function symbolByIndex($index) {
    if (array_key_exists($index, $this->symbols)) {
      return $this->symbols[$index];
    }
    throw new AvroException(sprintf('Invalid symbol index %d', $index));
  }

  /**
   * @param string $symbol
   * @return int the index of the given $symbol in the enum schema
   * @throws AvroException in case the symbol is not defined in the list available
   */
  public function symbolIndex($symbol) {
    $idx = array_search($symbol, $this->symbols, true);
    if (false !== $idx) {
      return $idx;
    }
    throw new AvroException(sprintf("Invalid symbol value '%s'", $symbol));
  }

  /**
   * Checks to see if the datum is a symbol defined in the current enum or not
   * @param string|AvroEnumRecord $datum the data which should be checked
   * @return bool true if the value is possible for this enum
   */
  public function isValidDatum($datum) {
    return $this->hasSymbol((string)$datum); // we force to string for the AvroEnumRecord type
  }

  /**
   * Writes the passed datum value as an index for the current enum on the encoder
   * @param mixed $datum the value to be written
   * @param AvroIOBinaryEncoder $encoder the encoder which should be used for the write
   * @throws AvroIOException in case of error while writing
   * @throws AvroException in case that the value passed is not possible for the current enum
   */
  public function writeDatum($datum, AvroIOBinaryEncoder $encoder) {
    $encoder->writeInt($this->symbolIndex((string)$datum));
  }

  /**
   * Checks to see if the the readersSchema is compatible with the current writersSchema ($this)
   * @param AvroSchema $readersSchema other schema to be checked with
   * @return boolean true if this schema is compatible with the readersSchema supplied
   */
  public function schemaMatches(AvroSchema $readersSchema) {
    if ($readersSchema instanceof AvroEnumSchema) {
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
    if ($readersSchema instanceof AvroEnumSchema) {
      $symbolIndex = $decoder->readInt();
      $symbol = $this->symbolByIndex($symbolIndex);
      return AvroRecordHelper::getNewEnumInstance($this, $symbol);
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
    $decoder->skipInt();
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
    return AvroRecordHelper::getNewEnumInstance($this, $defaultValue);
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::SYMBOLS_ATTR] = $this->symbols;
    return $avro;
  }
}
