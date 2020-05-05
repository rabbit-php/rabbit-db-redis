<?php
declare(strict_types=1);

namespace rabbit\db\redis;

use rabbit\App;
use rabbit\core\Exception;

/**
 * Class SentinelsManager
 * @package rabbit\db\redis
 */
class SentinelsManager
{
    const LOG_KEY = 'redis';
    /** @var int */
    protected $size;
    /** @var \SplQueue */
    protected $queue;
    /** @var int */
    protected $current = 0;
    /** @var \SplQueue */
    protected $wait;

    /**
     * SentinelsManager constructor.
     * @throws \Exception
     */
    public function __construct()
    {
        $this->queue = new \SplQueue();
        $this->wait = new \SplQueue();
    }

    /**
     * @param array $sentinels
     * @param string $type
     * @param string $masterName
     * @return mixed
     * @throws Exception
     */
    public function discover(array $sentinels, string $type, string $masterName = 'mymaster')
    {
        $this->size = count($sentinels);
        foreach ($sentinels as $sentinel) {
            if (is_scalar($sentinel)) {
                $sentinel = [
                    'hostname' => $sentinel
                ];
            }
            $key = $sentinel['hostname'] . (isset($sentinel['port']) ? ':' . $sentinel['port'] : '');

            if (!$this->queue->isEmpty()) {
                $connection = $this->queue->shift();
            } elseif ($this->current >= $this->size) {
                $this->wait->push(\Co::getCid());
                \Co::yield();
                $connection = $this->queue->shift();
            } else {
                try {
                    $this->current++;
                    $connection = new SentinelConnection();
                    $connection->hostname = isset($sentinel['hostname']) ? $sentinel['hostname'] : null;
                    $connection->masterName = $masterName;
                    if (isset($sentinel['port'])) {
                        $connection->port = $sentinel['port'];
                    }
                    $connection->connectionTimeout = isset($sentinel['connectionTimeout']) ? $sentinel['connectionTimeout'] : null;
                    $connection->unixSocket = isset($sentinel['unixSocket']) ? $sentinel['unixSocket'] : null;
                } catch (\Throwable $exception) {
                    $this->current--;
                    throw new Exception("can not open sentinel, $key");
                }

            }
            $method = 'get' . ucfirst($type);
            $res = $connection->$method();
            $r = [];
            if (!is_array($res[0])) {
                $res = [$res];
            }
            foreach ($res as $index => $item) {
                for ($i = 0; $i < count($item); $i += 2) {
                    $r[$index][$item[$i]] = $item[$i + 1];
                }
            }
            $r = $r[array_rand($r)];
            if (isset($sentinel['hostname'])) {
                $connectionName = "{$connection->hostname}:{$connection->port}";
            } else {
                $connectionName = $connection->unixSocket;
            }
            if ($this->queue->count() < $this->size) {
                $this->queue->push($connection);
            } else {
                $this->current--;
            }
            if ($this->wait->count() > 0) {
                $cid = $this->wait->shift();
                \Co::resume($cid);
            }
            if ($r) {
                App::info("Sentinel @{$connectionName} gave master addr: {$r['ip']}:{$r['port']}", self::LOG_KEY);
                return [$r['ip'], $r['port']];
            } else {
                App::error("Did not get any master from sentinel @{$connectionName}", self::LOG_KEY);
            }
        }
        throw new \Exception("$key Master could not be discovered");
    }
}