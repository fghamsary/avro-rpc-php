<?php


use Avro\Record\AvroRecord;

class TestProtocol {
  const PROTOCOL_JSON = <<<'DATUM'
{
  "namespace": "fr.v3d",
  "protocol": "TestProtocol",
  "types": [
    {"name": "FullName", "type": "record", "fields": [{"name": "firstName", "type": "string"}, {"name": "lastName", "type": "string"}]},
    {"name": "GreetingFull", "type": "record", "fields": [{"name": "message", "type": "string"}, {"name": "name", "type": "FullName"}]},
    {"name": "GreetingFullNullable", "type": "record", "fields": [{"name": "message", "type": "string"}, {"name": "name", "type": ["null", "FullName"]}]},
    {"name": "Greeting", "type": "record", "fields": [{"name": "message", "type": "string"}]},
    {"name": "UnionPrimitives", "type": "record", "fields": [{"name": "item", "type": ["null", "int", "float", "string"]}]},
    {"name": "InheritanceType1", "type": "record", "fields": [
      {"name": "partGeneral", "type": ["null", "string"]},
      {"name": "partSpec1", "type": "string"},
      {"name": "partSpec3", "type": ["null", "int"]}
    ]},
    {"name": "InheritanceType2", "type": "record", "fields": [
      {"name": "partGeneral", "type": ["null", "string"]},
      {"name": "partSpec1", "type": "string"},
      {"name": "partSpec4", "type": ["null", "float"]}
    ]},
    {"name": "InheritanceType3", "type": "record", "fields": [
      {"name": "partGeneral", "type": ["null", "string"]},
      {"name": "partSpec2", "type": "string"},
      {"name": "partSpec5", "type": ["null", "string"]}
    ]},
    {"name": "UnionInheritance", "type": "record", "fields": [
      {"name": "item", "type": ["null", "InheritanceType1", "InheritanceType2", "InheritanceType3"]}
    ]},
    {"name": "MultiItems", "type": "record", "fields": [
      {"name": "items", "type": ["null", {"type": "array", "items": ["InheritanceType1", "InheritanceType2", "InheritanceType3"]}]}
    ]},
    {"name": "MultiMapItems", "type": "record", "fields": [
      {"name": "items", "type": ["null", {"type": "map", "values": ["InheritanceType1", "InheritanceType2", "InheritanceType3"]}]}
    ]},
    {"name": "Curse", "type": "error", "fields": [{"name": "message", "type": "string"}]}
  ],
  "messages": {
    "hello": {
      "request": [{"name": "greeting", "type": "Greeting" }],
      "response": "Greeting",
      "errors": ["Curse"]
    }
  }
}
DATUM;
}

class FullName extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return 'FullName';
  }
  /**
   * @var string
   */
  private $firstName;
  /**
   * @var string
   */
  private $lastName;

  /**
   * @return string
   */
  public function getFirstName(): string {
    return $this->firstName;
  }

  /**
   * @param string $firstName
   */
  public function setFirstName(string $firstName): void {
    $this->firstName = $firstName;
  }

  /**
   * @return string
   */
  public function getLastName(): string {
    return $this->lastName;
  }

  /**
   * @param string $lastName
   */
  public function setLastName(string $lastName): void {
    $this->lastName = $lastName;
  }
}
class GreetingFullNullable extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return 'GreetingFullNullable';
  }
  /**
   * @var string
   */
  private $message;
  /**
   * @var FullName|null
   */
  private $name;
  /**
   * @return string
   */
  public function getMessage(): string {
    return $this->message;
  }
  /**
   * @param string $message
   */
  public function setMessage(string $message): void {
    $this->message = $message;
  }
  /**
   * @return FullName
   */
  public function getName(): ?FullName {
    return $this->name;
  }
  /**
   * @param FullName $name
   */
  public function setName(?FullName $name): void {
    $this->name = $name;
  }
}
class GreetingFull extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return 'GreetingFull';
  }
  /**
   * @var string
   */
  private $message;
  /**
   * @var FullName
   */
  private $name;
  /**
   * @return string
   */
  public function getMessage(): string {
    return $this->message;
  }
  /**
   * @param string $message
   */
  public function setMessage(string $message): void {
    $this->message = $message;
  }
  /**
   * @return FullName
   */
  public function getName(): FullName {
    return $this->name;
  }
  /**
   * @param FullName $name
   */
  public function setName(FullName $name): void {
    $this->name = $name;
  }
}
class Greeting extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return 'Greeting';
  }
  /**
   * @var string
   */
  private $message;
  /**
   * @return string
   */
  public function getMessage(): string {
    return $this->message;
  }
  /**
   * @param string $message
   */
  public function setMessage(string $message): void {
    $this->message = $message;
  }
}
class UnionPrimitives extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return 'UnionPrimitives';
  }
  /** @var mixed */
  private $item;
  /**
   * @return mixed
   */
  public function getItem() {
    return $this->item;
  }
  /**
   * @param mixed $item
   */
  public function setItem($item): void {
    $this->item = $item;
  }
}
abstract class ParentType extends AvroRecord {
  /** @var string|null */
  private $partGeneral;
  /**
   * @return string|null
   */
  public function getPartGeneral(): ?string {
    return $this->partGeneral;
  }
  /**
   * @param string|null $partGeneral
   * @return ParentType
   */
  public function setPartGeneral(?string $partGeneral): ParentType {
    $this->partGeneral = $partGeneral;
    return $this;
  }
  public function equals(ParentType $that): bool {
    return $this->getPartGeneral() === $that->getPartGeneral();
  }
}
abstract class SubParentType extends ParentType {
  /** @var string */
  private $partSpec1;
  /**
   * @return string
   */
  public function getPartSpec1(): string {
    return $this->partSpec1;
  }
  /**
   * @param string $partSpec1
   * @return SubParentType
   */
  public function setPartSpec1(string $partSpec1): SubParentType {
    $this->partSpec1 = $partSpec1;
    return $this;
  }
  public function equals(ParentType $that): bool {
    return $that instanceof SubParentType &&
      parent::equals($that) &&
      $this->getPartSpec1() === $that->getPartSpec1();
  }
}
/** @method static InheritanceType1 newInstance() */
class InheritanceType1 extends SubParentType {
  public static function _getSimpleAvroClassName(): string {
    return "InheritanceType1";
  }
  /** @var int|null */
  private $partSpec3;
  /**
   * @return int|null
   */
  public function getPartSpec3(): ?int {
    return $this->partSpec3;
  }
  /**
   * @param int|null $partSpec3
   * @return InheritanceType1
   */
  public function setPartSpec3(?int $partSpec3): InheritanceType1 {
    $this->partSpec3 = $partSpec3;
    return $this;
  }
  public function equals(ParentType $that): bool {
    return $that instanceof InheritanceType1 &&
      parent::equals($that) &&
      $this->getPartSpec3() === $that->getPartSpec3();
  }
}
/** @method static InheritanceType2 newInstance() */
class InheritanceType2 extends SubParentType {
  public static function _getSimpleAvroClassName(): string {
    return "InheritanceType2";
  }
  /** @var float|null */
  private $partSpec4;
  /**
   * @return float|null
   */
  public function getPartSpec4(): ?float {
    return $this->partSpec4;
  }
  /**
   * @param float|null $partSpec4
   * @return InheritanceType2
   */
  public function setPartSpec4(?float $partSpec4): InheritanceType2 {
    $this->partSpec4 = $partSpec4;
    return $this;
  }
  public function equals(ParentType $that): bool {
    return $that instanceof InheritanceType2 &&
      parent::equals($that) &&
      $this->getPartSpec4() === $that->getPartSpec4();
  }
}
/** @method static InheritanceType3 newInstance() */
class InheritanceType3 extends ParentType {
  public static function _getSimpleAvroClassName(): string {
    return "InheritanceType3";
  }
  /** @var string */
  private $partSpec2;
  /** @var string|null */
  private $partSpec5;
  /**
   * @return string
   */
  public function getPartSpec2(): string {
    return $this->partSpec2;
  }
  /**
   * @param string $partSpec2
   * @return InheritanceType3
   */
  public function setPartSpec2(string $partSpec2): InheritanceType3 {
    $this->partSpec2 = $partSpec2;
    return $this;
  }
  /**
   * @return string|null
   */
  public function getPartSpec5(): ?string {
    return $this->partSpec5;
  }
  /**
   * @param string|null $partSpec5
   * @return InheritanceType3
   */
  public function setPartSpec5(?string $partSpec5): InheritanceType3 {
    $this->partSpec5 = $partSpec5;
    return $this;
  }
  public function equals(ParentType $that): bool {
    return $that instanceof InheritanceType3 &&
      parent::equals($that) &&
      $this->getPartSpec2() === $that->getPartSpec2() &&
      $this->getPartSpec5() === $that->getPartSpec5();
  }
}
class UnionInheritance extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return "UnionInheritance";
  }
  /** @var ParentType|null */
  private $item;
  /**
   * @return ParentType|null
   */
  public function getItem(): ?ParentType {
    return $this->item;
  }
  /**
   * @param ParentType|null $item
   */
  public function setItem(?ParentType $item): void {
    $this->item = $item;
  }
}
class MultiItems extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return 'MultiItems';
  }
  /** @var ParentType[] */
  private $items;
  /**
   * @return ParentType[]
   */
  public function getItems(): array {
    return $this->items;
  }
  /**
   * @param ParentType[] $items
   */
  public function setItems(array $items): void {
    $this->items = $items;
  }
}
class MultiMapItems extends AvroRecord {
  public static function _getSimpleAvroClassName(): string {
    return 'MultiMapItems';
  }
  /** @var ParentType[] */
  private $items;
  /**
   * @return ParentType[]
   */
  public function getItems(): array {
    return $this->items;
  }
  /**
   * @param ParentType[] $items
   */
  public function setItems(array $items): void {
    $this->items = $items;
  }
}
