<?php
declare(strict_types=1);

namespace rabbit\db\redis;

use rabbit\core\ObjectFactory;
use rabbit\db\ConnectionInterface;
use rabbit\db\redis\pool\RedisPool;
use rabbit\db\redis\pool\RedisPoolConfig;
use rabbit\helper\ArrayHelper;

/**
 * Class Manager
 * @package rabbit\db\redis
 */
class Manager
{
    /** @var RedisPool[] */
    private $connections = [];
    /** @var array */
    private $yamlList = [];
    /** @var int */
    private $min = 48;
    /** @var int */
    private $max = 56;
    /** @var int */
    private $wait = 0;

    /**
     * Manager constructor.
     * @param array $configs
     */
    public function __construct(array $configs = [])
    {
        $this->addConnection($configs);
    }

    /**
     * @param array $configs
     */
    public function addConnection(array $configs): void
    {
        foreach ($configs as $name => $config) {
            if (!isset($this->connections[$name])) {
                $this->connections[$name] = ObjectFactory::createObject($config, [], false);
            }
        }
    }

    /**
     * @param string $name
     * @return ConnectionInterface|null
     */
    public function getConnection(string $name = 'db'): ?ConnectionInterface
    {
        /** @var RedisPool $pool */
        if (!isset($this->connections[$name])) {
            if (empty($this->yamlList)) {
                return null;
            }
            $this->createByYaml();
        }
        $redis = $this->connections[$name];
        return $redis;
    }

    /**
     * @param string $name
     * @return bool
     */
    public function hasConnection(string $name): bool
    {
        return isset($this->connections[$name]);
    }

    private function createByYaml(): void
    {
        foreach ($this->yamlList as $fileName) {
            foreach (yaml_parse_file($fileName) as $name => $dbconfig) {
                if (!isset($this->connections[$name])) {
                    if (!isset($dbconfig['class']) || !isset($dbconfig['dsn']) ||
                        !class_exists($dbconfig['class']) || !$dbconfig['class'] instanceof ConnectionInterface) {
                        throw new Exception("The DB class and dsn must be set current class in $fileName");
                    }
                    [$min, $max, $wait] = ArrayHelper::getValueByArray(
                        ArrayHelper::getValue($dbconfig, 'pool', []),
                        ['min', 'max', 'wait'],
                        null,
                        $this->min,
                        $this->max,
                        $this->wait
                    );
                    ArrayHelper::removeKeys($dbconfig, 'class', 'dsn', 'pool');
                    $this->connections[$name] = ObjectFactory::createObject([
                        'class' => str_replace('Connection', 'Redis', $dbconfig['class']),
                        'pool' => ObjectFactory::createObject([
                            'class' => RedisPool::class,
                            'connection' => $dbconfig['class'],
                            'poolConfig' => ObjectFactory::createObject([
                                'class' => RedisPoolConfig::class,
                                'minActive' => intval($min / swoole_cpu_num()),
                                'maxActive' => intval($max / swoole_cpu_num()),
                                'maxWait' => $wait,
                                'uri' => [$dbconfig['dsn']]
                            ], [], false)
                        ], $dbconfig, false)
                    ]);
                }
            }
        }
    }
}