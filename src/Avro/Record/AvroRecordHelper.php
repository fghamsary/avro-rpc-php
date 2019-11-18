<?php
/**
 * Created by IntelliJ IDEA.
 * User: fghamsary
 * Date: 05/01/2019
 * Time: 14:26
 */

namespace Avro\Record;


use Avro\Exception\AvroException;
use Avro\Schema\AvroEnumSchema;
use Avro\Schema\AvroRecordSchema;

class AvroRecordHelper {

  /**
   * @var string The default namespace which should be used to search for classes while reading from binary
   */
  private static $defaultNamespace = '\\';

  /**
   * @var IAvroRecordInstantiator|null the instantiator which can be used instead of default record instantiator
   */
  private static $recordInstantiator = null;

  /**
   * @return string The default namespace which should be used to search for classes while reading from binary
   */
  public static function getDefaultNamespace(): string {
    return self::$defaultNamespace;
  }

  /**
   * @param string $defaultNamespace The default namespace which should be used to search for classes while reading from binary
   */
  public static function setDefaultNamespace(string $defaultNamespace): void {
    self::$defaultNamespace = $defaultNamespace;
  }

  /**
   * @return IAvroRecordInstantiator|null the default instantiator for the schema defined
   */
  public static function getRecordInstantiator(): ?IAvroRecordInstantiator {
    return self::$recordInstantiator;
  }

  /**
   * @param IAvroRecordInstantiator|null $recordInstantiator the default instantiator for the defined schema
   */
  public static function setRecordInstantiator(?IAvroRecordInstantiator $recordInstantiator): void {
    self::$recordInstantiator = $recordInstantiator;
  }

  /**
   * Creates new instance of the enum record if the corresponding class exists or string if not
   *
   * @param AvroEnumSchema $schema the schema for which the enum instance should be created
   * @param string $symbol the symbol for which the corresponding object should be returned
   *
   * @return AvroEnumRecord|string the result will be an instance of AvroEnumRecord if the corresponding class exists and a string if not
   * @throws AvroException if the class exists but the value is not defined for this enum class
   */
  public static function getNewEnumInstance(AvroEnumSchema $schema, string $symbol) {
    $enumName = self::getDefaultNamespace() . $schema->getQualifiedName();
    if (class_exists($enumName)) {
      /** @var AvroEnumRecord $enumName */
      $symbol = $enumName::getItem($symbol);
    }
    return $symbol;
  }

  /**
   * Creates new instance of the IAvroRecordBase if the corresponding class exists or a simple array if not
   *
   * @param AvroRecordSchema $schema the schema for which the record instance should be created
   *
   * @return array|IAvroRecordBase the result will be an instance of IAvroRecordBase if the corresponding class exists and an array if not
   */
  public static function getNewRecordInstance(AvroRecordSchema $schema) {
    if (self::$recordInstantiator !== null) {
      $result = self::$recordInstantiator->getNewRecordInstance(self::getDefaultNamespace(), $schema);
      if ($result !== null) {
        return $result;
      }
    }
    /** @var IAvroRecordBase $recordClassName */
    $recordClassName = self::getDefaultNamespace() . $schema->getQualifiedName();
    if (class_exists($recordClassName)) {
      /** @var IAvroRecordBase $record */
      return $recordClassName::newInstance();
    } else {
      return [];
    }
  }

}