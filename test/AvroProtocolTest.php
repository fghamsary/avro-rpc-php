<?php

use Avro\Exception\AvroException;
use Avro\Protocol\AvroProtocol;

require_once 'TestProtocol/ProtocolClasses.php';


class AvroProtocolTest extends PHPUnit\Framework\TestCase {

  private $protocol;

  public function setUp() {
    parent::setUp();
    $this->protocol = AvroProtocol::parse(TestProtocol::PROTOCOL_JSON);
    $this->assertNotNull($this->protocol);
  }

  public function testSerializationDeserialization() {

    $newGreeting = new Greeting();
    $newGreeting->setMessage('Test Message');
    $result = $this->protocol->serializeObject($newGreeting);

    $this->assertNotNull($result);
    $this->assertIsArray($result);
    $this->assertArrayHasKey('message', $result);
    $this->assertEquals('Test Message', $result['message']);

    $greeting2 = new Greeting();
    $this->protocol->deserializeObject($greeting2, $result);

    $this->assertEquals('Test Message', $greeting2->getMessage());
  }

  public function testSerializationDeserializationDeep() {
    $greetingFull = [
      'message' => 'Test',
      'name' => [
        'firstName' => 'First',
        'lastName' => 'Last'
      ]
    ];

    $greetingFullObject = new GreetingFull();
    $this->protocol->deserializeObject($greetingFullObject, $greetingFull);
    $this->assertNotNull($greetingFull);
    $this->assertEquals('Test', $greetingFullObject->getMessage());
    $this->assertNotNull($greetingFullObject->getName());
    $this->assertInstanceOf(FullName::class, $greetingFullObject->getName());
    $this->assertEquals('First', $greetingFullObject->getName()->getFirstName());
    $this->assertEquals('Last', $greetingFullObject->getName()->getLastName());

    $greetingFullObject = new GreetingFullNullable();
    $this->protocol->deserializeObject($greetingFullObject, $greetingFull);
    $this->assertNotNull($greetingFull);
    $this->assertEquals('Test', $greetingFullObject->getMessage());
    $this->assertNotNull($greetingFullObject->getName());
    $this->assertInstanceOf(FullName::class, $greetingFullObject->getName());
    $this->assertEquals('First', $greetingFullObject->getName()->getFirstName());
    $this->assertEquals('Last', $greetingFullObject->getName()->getLastName());

    $greetingFullObject = new GreetingFullNullable();
    $greetingFull['name'] = null;
    $this->protocol->deserializeObject($greetingFullObject, $greetingFull);
    $this->assertNotNull($greetingFull);
    $this->assertEquals('Test', $greetingFullObject->getMessage());
    $this->assertNull($greetingFullObject->getName());

    $greetingFullObject = new GreetingFullNullable();
    unset($greetingFull['name']);
    $this->protocol->deserializeObject($greetingFullObject, $greetingFull);
    $this->assertNotNull($greetingFull);
    $this->assertEquals('Test', $greetingFullObject->getMessage());
    $this->assertNull($greetingFullObject->getName());
  }

  public function testSerializeDeserializeUnionPrimitives() {
    $union = new UnionPrimitives();
    $json = ['item' => 'string'];
    $this->protocol->deserializeObject($union, $json);
    $this->assertEquals('string', $union->getItem());
    $this->assertEquals($json, $this->protocol->serializeObject($union));

    $union = new UnionPrimitives();
    $json = ['item' => 1234];
    $this->protocol->deserializeObject($union, $json);
    $this->assertEquals(1234, $union->getItem());
    $this->assertEquals($json, $this->protocol->serializeObject($union));

    $union = new UnionPrimitives();
    $json = ['item' => 1234.45];
    $this->protocol->deserializeObject($union, $json);
    $this->assertEquals(1234.45, $union->getItem());
    $this->assertEquals($json, $this->protocol->serializeObject($union));

    $union = new UnionPrimitives();
    $json = ['item' => null];
    $this->protocol->deserializeObject($union, $json);
    $this->assertNull($union->getItem());
    $this->assertEquals($json, $this->protocol->serializeObject($union));

    $union = new UnionPrimitives();
    $this->assertEquals($json, $this->protocol->serializeObject($union));
  }

  public function testSerializeDeserializeUnionPrimitivesException() {
    $union = new UnionPrimitives();
    $this->expectException(AvroException::class);
    $this->protocol->deserializeObject($union, ['item' => ['test']]);

    $union = new UnionPrimitives();
    try {
      $this->protocol->deserializeObject($union, ['item' => []]);
      $this->fail('We should have an exception!');
    } catch (AvroException $exp) {

    }
  }

  public function testSerializationDeserializeUnionInheritance() {
    $json = [
      'item' => [
        'partGeneral' => 'test',
        'partSpec1' => 'spec',
        'partSpec4' => 14.5
      ]
    ];
    $obj = new UnionInheritance();
    $this->protocol->deserializeObject($obj, $json);
    /** @var InheritanceType2 $item */
    $item = $obj->getItem();
    $this->assertInstanceOf(InheritanceType2::class, $item);
    $this->assertEquals('test', $item->getPartGeneral());
    $this->assertEquals('spec', $item->getPartSpec1());
    $this->assertEquals(14.5, $item->getPartSpec4());

    $jsonSerialized = $this->protocol->serializeObject($obj);
    $this->assertIsArray($jsonSerialized);
    $this->assertArrayHasKey('item', $jsonSerialized);
    $this->assertEquals($item, $jsonSerialized['item']);
    $this->assertEquals($json, $this->protocol->serializeObject($obj, true));
  }

  public function testSerializationUnionInheritance() {
    $multiItems = new MultiItems();
    $items = [];
    $items[] = InheritanceType1::newInstance()
      ->setPartSpec3(11)
      ->setPartSpec1('spec1-1')
      ->setPartGeneral('gen1-1');
    $items[] = InheritanceType3::newInstance()
      ->setPartSpec2('spec2-31')
      ->setPartSpec5('spec5-31')
      ->setPartGeneral('gen3-1');
    $items[] = InheritanceType2::newInstance()
      ->setPartSpec4(21.1)
      ->setPartSpec1('spec2-1')
      ->setPartGeneral('gen2-1');
    $items[] = InheritanceType1::newInstance()
      ->setPartSpec3(12)
      ->setPartSpec1('spec1-2')
      ->setPartGeneral('gen1-2');
    $items[] = InheritanceType2::newInstance()
      ->setPartSpec4(22.2)
      ->setPartSpec1('spec2-2')
      ->setPartGeneral('gen2-2');
    $items[] = InheritanceType3::newInstance()
      ->setPartSpec2('spec2-32')
      ->setPartSpec5('spec5-32')
      ->setPartGeneral('gen3-2');
    $items[] = InheritanceType2::newInstance()
      ->setPartSpec1('spec2-3')
      ->setPartGeneral('gen2-3');
    $items[] = InheritanceType1::newInstance()
      ->setPartSpec1('spec1-3')
      ->setPartGeneral('gen1-3');

    $multiItems->setItems($items);

    $json = $this->protocol->serializeObject($multiItems, true);
    $this->assertIsArray($json);
    $this->assertArrayHasKey('items', $json);
    $this->assertIsArray($json['items']);

    $multiItemsNew = new MultiItems();
    $this->protocol->deserializeObject($multiItemsNew, $json);
    $itemsNew = $multiItemsNew->getItems();
    $this->assertIsArray($itemsNew);
    $this->assertCount(8, $itemsNew);
    $this->assertInstanceOf(InheritanceType1::class, $itemsNew[0]);
    $this->assertInstanceOf(InheritanceType3::class, $itemsNew[1]);
    $this->assertInstanceOf(InheritanceType2::class, $itemsNew[2]);
    $this->assertInstanceOf(InheritanceType1::class, $itemsNew[3]);
    $this->assertInstanceOf(InheritanceType2::class, $itemsNew[4]);
    $this->assertInstanceOf(InheritanceType3::class, $itemsNew[5]);
    $this->assertInstanceOf(InheritanceType2::class, $itemsNew[6]);
    $this->assertInstanceOf(InheritanceType1::class, $itemsNew[7]);

    for ($i = 0; $i < 8; $i++) {
      $this->assertTrue($items[$i]->equals($itemsNew[$i]));
    }

    // this is an instance of InheritanceType1 or InheritanceType2 because it's generic
    // but will be deserialized as InheritanceType1
    $json['items'][] = [
      'partSpec1' => 'spec1-4',
      'partGeneral' => 'gen1-4'
    ];
    try {
      $this->protocol->deserializeObject($multiItemsNew, $json);
      $this->fail('It should not be parsable as it is not a correct item!');
    } catch (AvroException $exp) {
    }
  }

  public function testSerializationUnionMapInheritance() {
    $multiItems = new MultiMapItems();
    $items = [];
    $items['i11'] = InheritanceType1::newInstance()
      ->setPartSpec3(11)
      ->setPartSpec1('spec1-1')
      ->setPartGeneral('gen1-1');
    $items['i31'] = InheritanceType3::newInstance()
      ->setPartSpec2('spec2-31')
      ->setPartSpec5('spec5-31')
      ->setPartGeneral('gen3-1');
    $items['i21'] = InheritanceType2::newInstance()
      ->setPartSpec4(21.1)
      ->setPartSpec1('spec2-1')
      ->setPartGeneral('gen2-1');
    $items['i12'] = InheritanceType1::newInstance()
      ->setPartSpec3(12)
      ->setPartSpec1('spec1-2')
      ->setPartGeneral('gen1-2');
    $items['i22'] = InheritanceType2::newInstance()
      ->setPartSpec4(22.2)
      ->setPartSpec1('spec2-2')
      ->setPartGeneral('gen2-2');
    $items['i32'] = InheritanceType3::newInstance()
      ->setPartSpec2('spec2-32')
      ->setPartSpec5('spec5-32')
      ->setPartGeneral('gen3-2');
    $items['i23'] = InheritanceType2::newInstance()
      ->setPartSpec1('spec2-3')
      ->setPartGeneral('gen2-3');
    $items['i13'] = InheritanceType1::newInstance()
      ->setPartSpec1('spec1-3')
      ->setPartGeneral('gen1-3');

    $multiItems->setItems($items);

    $json = $this->protocol->serializeObject($multiItems, false);
    $this->assertIsArray($json);
    $this->assertArrayHasKey('items', $json);
    $this->assertIsArray($json['items']);
    foreach ($items as $key => $value) {
      $this->assertArrayHasKey($key, $json['items']);
      $this->assertEquals($value, $json['items'][$key]);
    }

    $json = $this->protocol->serializeObject($multiItems, true, false);
    $this->assertIsArray($json);
    $this->assertArrayHasKey('items', $json);
    $this->assertIsArray($json['items']);
    $i = 0;
    foreach ($items as $key => $value) {
      $this->assertArrayNotHasKey($key, $json['items']);
      $this->assertEquals($value->getPartGeneral(), $json['items'][$i++]['partGeneral']);
    }

    $json = $this->protocol->serializeObject($multiItems, true);
    $this->assertIsArray($json);
    $this->assertArrayHasKey('items', $json);
    $this->assertIsArray($json['items']);
    foreach ($items as $key => $value) {
      $this->assertArrayHasKey($key, $json['items']);
    }

    $multiItemsNew = new MultiMapItems();
    $this->protocol->deserializeObject($multiItemsNew, $json);
    $itemsNew = $multiItemsNew->getItems();
    $this->assertIsArray($itemsNew);
    $this->assertCount(8, $itemsNew);
    $this->assertInstanceOf(InheritanceType1::class, $itemsNew['i11']);
    $this->assertInstanceOf(InheritanceType3::class, $itemsNew['i31']);
    $this->assertInstanceOf(InheritanceType2::class, $itemsNew['i21']);
    $this->assertInstanceOf(InheritanceType1::class, $itemsNew['i12']);
    $this->assertInstanceOf(InheritanceType2::class, $itemsNew['i22']);
    $this->assertInstanceOf(InheritanceType3::class, $itemsNew['i32']);
    $this->assertInstanceOf(InheritanceType2::class, $itemsNew['i23']);
    $this->assertInstanceOf(InheritanceType1::class, $itemsNew['i13']);

    foreach ($items as $key => $value) {
      $this->assertTrue($value->equals($itemsNew[$key]));
    }

    // this is an instance of InheritanceType1 or InheritanceType2 because it's generic
    // but will be deserialized as InheritanceType1
    $json['items'][] = [
      'partSpec1' => 'spec1-4',
      'partGeneral' => 'gen1-4'
    ];
    try {
      $this->protocol->deserializeObject($multiItemsNew, $json);
      $this->fail('It should not be parsable as it is not a correct item!');
    } catch (AvroException $exp) {
    }
  }

}
