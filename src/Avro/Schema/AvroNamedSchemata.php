<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:14
 */

namespace Avro\Schema;

use Avro\AvroUtil;
use Avro\Exception\AvroSchemaParseException;

/**
 * Class AvroNamedSchemata
 *
 * Keeps track of AvroNamedSchema which have been observed so far, as well as the default namespace.
 *
 * @package Avro\Schema
 */
class AvroNamedSchemata {
  /**
   * @var AvroNamedSchema[]
   */
  private $schemata;

  /**
   * @param AvroNamedSchemata[] $schemata
   */
  public function __construct($schemata = array()) {
    $this->schemata = $schemata;
  }

  public function listSchemas() {
    var_export($this->schemata);
    foreach ($this->schemata as $sch)
      print('Schema ' . $sch->__toString() . "\n");
  }

  /**
   * @param string $fullname
   * @return boolean true if there exists a schema with the given name and false otherwise.
   */
  public function hasName($fullname) {
    return array_key_exists($fullname, $this->schemata);
  }

  /**
   * @param string $fullname
   * @return AvroSchema|null the schema which has the given name,
   *         or null if there is no schema with the given name.
   */
  public function schema($fullname) {
    return AvroUtil::arrayValue($this->schemata, $fullname);
  }

  /**
   * @param AvroName $name
   * @return AvroSchema|null
   */
  public function schemaByName(AvroName $name) {
    return $this->schema($name->getFullname());
  }

  /**
   * Creates a new AvroNamedSchemata instance of this schemata instance
   * with the given $schema appended.
   * @param AvroNamedSchema $schema to add to this existing schemata
   * @return AvroNamedSchemata
   * @throws AvroSchemaParseException
   */
  public function cloneWithNewSchema(AvroNamedSchema $schema) {
    $name = $schema->getFullname();
    if (AvroSchema::isValidType($name)) {
      throw new AvroSchemaParseException(sprintf('Name "%s" is a reserved type name', $name));
    } elseif ($this->hasName($name)) {
      throw new AvroSchemaParseException(sprintf('Name "%s" is already in use', $name));
    }
    $schemata = new AvroNamedSchemata($this->schemata);
    $schemata->schemata[$name] = $schema;
    return $schemata;
  }

  /**
   * @return string[] returns the list of records in the schema
   */
  public function getSchemaNames() {
    return array_keys($this->schemata);
  }

  public function toAvro() {
    $toAvro = [];
    foreach ($this->schemata as $fullname => $schema)
      $toAvro[] = $schema->toAvro();
    return $toAvro;
  }
}
