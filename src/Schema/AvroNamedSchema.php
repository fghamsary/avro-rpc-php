<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:21
 */

namespace Avro\Schema;

use Avro\Exception\AvroSchemaParseException;

/**
 * Class AvroNamedSchema
 *
 * Parent class of named Avro schema
 *
 * @package Avro\Schema
 */
abstract class AvroNamedSchema extends AvroSchema {
  /**
   * @var AvroName $name
   */
  private $name;

  /**
   * @var string documentation string
   */
  private $doc;

  /**
   * @param string $type
   * @param AvroName $name
   * @param string $doc documentation string
   * @param AvroNamedSchemata &$schemata
   * @throws AvroSchemaParseException
   */
  public function __construct($type, $name, $doc = null, &$schemata = null) {
    parent::__construct($type);
    $this->name = $name;

    if ($doc && !is_string($doc)) {
      throw new AvroSchemaParseException('Schema doc attribute must be a string');
    }
    $this->doc = $doc;

    if ($schemata !== null) {
      $schemata = $schemata->cloneWithNewSchema($this);
    }
  }

  /**
   * @return array
   */
  public function toAvro() {
    $avro = parent::toAvro();
    $avro[AvroSchema::NAME_ATTR] = $this->name->getName();
    if ($this->name->getNamespace() !== null) {
      $avro[AvroSchema::NAMESPACE_ATTR] = $this->name->getNamespace();
    }
    if ($this->doc !== null) {
      $avro[AvroSchema::DOC_ATTR] = $this->doc;
    }
    return $avro;
  }

  /**
   * @return string
   */
  public function getFullname() {
    return $this->name->getFullname();
  }

  /**
   * @return string teh
   */
  public function getQualifiedName() {
    return $this->name->getQualifiedName();
  }

}
