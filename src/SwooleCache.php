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
 * Class SwooleCache
 * @package rabbit\db\redis
 */
class SwooleCache extends AbstractCache implements CacheInterface
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
        $result = $this->client->get($key);
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
            return (bool)$this->client->set($key, $value);
        } else {
            $ttl = (int)($ttl * 1000);

            return (bool)$this->client->set($key, $value, ['PX' => $ttl]);
        }
    }

    /**
     * @param string $key
     * @return bool
     */
    public function delete($key)
    {
        $this->buildKey($key);
        return (bool)$this->client->delete($key);
    }

    /**
     * @return bool
     */
    public function clear()
    {
        return $this->client->flushDB();
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
            $newKeys[$this->buildKey($key)] = $key;
        }
        $mgetResult = $this->client->mget(array_keys($newKeys));
        if ($mgetResult === false) {
            return $default;
        }
        $result = [];
        foreach ($mgetResult ?? [] as $key => $value) {
            $result[$newKeys[$key]] = $value;
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
        $newKeys = [];
        foreach ($values as $key => $value) {
            $args[$this->buildKey($key)] = $value;
            $newKeys[$this->buildKey($key)] = $key;
        }

        $failedKeys = [];
        $hash = array_shift($args);
        if ($ttl === 0) {
            $this->client->executeCommand('MSET', [$hash, $args]);
        } else {
            $ttl = (int)($ttl * 1000);
            $this->client->executeCommand('MULTI');
            $this->client->executeCommand('MSET', [$hash, $args]);
            $index = [];
            foreach ($args as $key => $value) {
                $this->client->executeCommand('PEXPIRE', [$key, $ttl]);
                $index[] = $newKeys[$key];
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
        return (bool)$this->client->del($newKeys);
    }

    /**
     * @param string $key
     * @return bool
     */
    public function has($key)
    {
        return $this->client->exists($this->buildKey($key));
    }
}
