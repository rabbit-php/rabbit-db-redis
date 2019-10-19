<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/26
 * Time: 23:57
 */

namespace rabbit\db\redis;

use Psr\SimpleCache\CacheInterface;
use rabbit\cache\AbstractCache;
use rabbit\core\ObjectFactory;

/**
 * Class Cache
 * @package rabbit\db\redis
 */
class Cache extends AbstractCache implements CacheInterface
{
    /** @var Redis */
    private $client;

    /**
     * Cache constructor.
     * @throws \Exception
     */
    public function __construct()
    {
        parent::__construct();
        $this->client = ObjectFactory::get('redis');
    }

    /**
     * @param string $key
     * @param null $default
     * @return mixed|null
     */
    public function get($key, $default = null)
    {
        $key = $this->buildKey($key);
        $result = $this->client->executeCommand('GET', [$key]);
        if ($result === false || $result === null) {
            return $default;
        }

        return $result;
    }

    /**
     * @param string $key
     * @param mixed $value
     * @param null $ttl
     * @return bool
     */
    public function set($key, $value, $ttl = null)
    {
        $key = $this->buildKey($key);
        if ($ttl === 0) {
            return (bool)$this->client->executeCommand('SET', [$key, $value]);
        } else {
            $ttl = (int)($ttl * 1000);

            return (bool)$this->client->executeCommand('SET', [$key, $value, 'PX', $ttl]);
        }
    }

    /**
     * @param string $key
     * @return bool
     */
    public function delete($key)
    {
        $this->buildKey($key);
        return (bool)$this->client->executeCommand('DEL', [$key]);
    }

    /**
     * @return bool
     */
    public function clear()
    {
        return $this->client->executeCommand('FLUSHDB');
    }

    /**
     * @param iterable $keys
     * @param null $default
     * @return array|iterable|null
     */
    public function getMultiple($keys, $default = null)
    {
        $newKeys = [];
        foreach ($keys as $key) {
            $newKeys[] = $this->buildKey($key);
        }
        $response = $this->client->executeCommand('MGET', $newKeys);
        $result = [];
        $i = 0;
        foreach ($keys as $key) {
            $result[$key] = $response[$i++];
        }

        return $result;
    }

    /**
     * @param iterable $values
     * @param null $ttl
     * @return bool
     */
    public function setMultiple($values, $ttl = null)
    {
        $args = [];
        foreach ($values as $key => $value) {
            $args[] = $this->buildKey($key);
            $args[] = $value;
        }

        $failedKeys = [];
        if ($ttl == 0) {
            $this->client->executeCommand('MSET', $args);
        } else {
            $ttl = (int)($ttl * 1000);
            $this->client->executeCommand('MULTI');
            $this->client->executeCommand('MSET', $args);
            $index = [];
            foreach ($values as $key => $value) {
                $this->client->executeCommand('PEXPIRE', [$key, $ttl]);
                $index[] = $key;
            }
            $result = $this->client->executeCommand('EXEC');
            array_shift($result);
            foreach ($result as $i => $r) {
                if ($r != 1) {
                    $failedKeys[] = $index[$i];
                }
            }
        }

        // FIXME: Where do we access failed keys from ?
        return count($failedKeys) === 0;
    }

    /**
     * @param iterable $keys
     * @return bool
     */
    public function deleteMultiple($keys)
    {
        $newKeys = [];
        foreach ($keys as $key) {
            $newKeys[] = $this->buildKey($key);
        }
        return (bool)$this->client->executeCommand('DEL', $newKeys);
    }

    /**
     * @param string $key
     * @return bool
     */
    public function has($key)
    {
        $value = $this->getValue($this->buildKey($key));
        return $value !== false;
    }
}
