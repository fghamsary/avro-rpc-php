<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 04/01/2019
 * Time: 11:24
 */

namespace Avro\Schema;


use Avro\Exception\AvroSchemaParseException;

/**
 * Avro name class to have correct name, namespace, fullname and qualified name for a named schema
 * Class AvroName
 * @package Avro\Schema
 */
class AvroName {

  /**
   * @var string character used to separate names comprising the fullname
   */
  const NAME_SEPARATOR = '.';

  /**
   * @var string regular expression to validate name values
   */
  const NAME_REGEXP = '/^[A-Za-z_][A-Za-z0-9_]*$/';

  /**
   * @param string $name the name of the record
   * @param string|null $namespace the namespace of the record
   * @return string[] array($name, $namespace)
   */
  public static function extractNamespace($name, $namespace = null) {
    $parts = explode(self::NAME_SEPARATOR, $name);
    if (count($parts) > 1) {
      $name = array_pop($parts);
      $namespace = join(self::NAME_SEPARATOR, $parts);
    }
    return array($name, $namespace);
  }

  /**
   * @param string $name the name to be checked against rules
   * @return boolean true if the given name is well-formed
   *          (is a non-null, non-empty string) and false otherwise
   */
  public static function isWellFormedName($name) {
    return (is_string($name) && !empty($name) && preg_match(self::NAME_REGEXP, $name));
  }

  /**
   * @param string $namespace
   * @return boolean true if namespace is composed of valid names
   * @throws AvroSchemaParseException if any of the namespace components
   *                                  are invalid.
   */
  private static function checkNamespaceNames($namespace) {
    foreach (explode(self::NAME_SEPARATOR, $namespace) as $n) {
      if (empty($n) || (0 == preg_match(self::NAME_REGEXP, $n))) {
        throw new AvroSchemaParseException(sprintf('Invalid name "%s"', $n));
      }
    }
    return true;
  }

  /**
   * @param string $name
   * @param string $namespace
   * @return string
   * @throws AvroSchemaParseException if any of the names are not valid.
   */
  private static function parseFullname($name, $namespace) {
    if (!is_string($namespace) || empty($namespace)) {
      throw new AvroSchemaParseException('Namespace must be a non-empty string.');
    }
    self::checkNamespaceNames($namespace);
    return $namespace . '.' . $name;
  }

  /**
   * @var string valid names are matched by self::NAME_REGEXP
   */
  private $name;

  /**
   * @var string
   */
  private $namespace;

  /**
   * @var string
   */
  private $fullname;

  /**
   * @var string Name qualified as necessary given its default namespace.
   */
  private $qualifiedName;

  /**
   * @param string $name the name schema used for this named schema
   * @param string|null $namespace the namespace defined for this named schema
   * @param string|null $defaultNamespace the default namespace for this protocol
   * @throws AvroSchemaParseException
   */
  public function __construct(string $name, string $namespace = null, string $defaultNamespace = null) {
    if (!is_string($name) || empty($name)) {
      throw new AvroSchemaParseException('Name must be a non-empty string.');
    }
    if (strpos($name, self::NAME_SEPARATOR) && self::checkNamespaceNames($name)) {
      $this->fullname = $name;
    } elseif (0 == preg_match(self::NAME_REGEXP, $name)) {
      throw new AvroSchemaParseException(sprintf('Invalid name "%s"', $name));
    } elseif ($namespace !== null) {
      $this->fullname = self::parseFullname($name, $namespace);
    } elseif ($defaultNamespace !== null) {
      $this->fullname = self::parseFullname($name, $defaultNamespace);
    } else {
      $this->fullname = $name;
    }
    $name_and_namespace = self::extractNamespace($this->fullname);
    $this->name = $name_and_namespace[0];
    $this->namespace = $name_and_namespace[1];
    $this->qualifiedName = ($this->namespace === null || $this->namespace === $defaultNamespace) ?
      $this->name :
      $this->fullname;
  }

  /**
   * @return string the name of the avro record
   */
  public function getName() {
    return $this->name;
  }

  /**
   * @return string the namespace of the avro record
   */
  public function getNamespace() {
    return $this->namespace;
  }

  /**
   * @return string
   */
  public function getFullname() {
    return $this->fullname;
  }

  /**
   * @returns string fullname
   * @uses $this->fullname()
   */
  public function __toString() {
    return $this->getFullname();
  }

  /**
   * @return string name qualified for its context
   */
  public function getQualifiedName() {
    return $this->qualifiedName;
  }

}
