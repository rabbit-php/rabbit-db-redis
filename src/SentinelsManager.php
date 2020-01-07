<?php


namespace rabbit\db\redis;

use rabbit\App;

/**
 * Class SentinelsManager
 * @package rabbit\db\redis
 */
class SentinelsManager
{
    const LOG_KEY = 'redis';
    /** @var array */
    private $sentinelList = [];

    /**
     * @param array $sentinels
     * @param string $masterName
     * @return array|false
     * @throws \Exception
     */
    public function discoverMaster(array $sentinels, string $masterName = 'mymaster')
    {
        foreach ($sentinels as $sentinel) {
            if (is_scalar($sentinel)) {
                $sentinel = [
                    'hostname' => $sentinel
                ];
            }
            $key = $sentinel['hostname'] . (isset($sentinel['port']) ? ':' . $sentinel['port'] : '');
            if (isset($this->sentinelList[$key])) {
                $connection = $this->sentinelList[$key];
            } else {
                $connection = new SentinelConnection();
                $connection->hostname = isset($sentinel['hostname']) ? $sentinel['hostname'] : null;
                $connection->masterName = $masterName;
                if (isset($sentinel['port'])) {
                    $connection->port = $sentinel['port'];
                }
                $connection->connectionTimeout = isset($sentinel['connectionTimeout']) ? $sentinel['connectionTimeout'] : null;
                $connection->unixSocket = isset($sentinel['unixSocket']) ? $sentinel['unixSocket'] : null;
                $this->sentinelList[$key] = $connection;
            }

            $r = $connection->getMaster();
            if (isset($sentinel['hostname'])) {
                $connectionName = "{$connection->hostname}:{$connection->port}";
            } else {
                $connectionName = $connection->unixSocket;
            }
            if ($r) {
                App::info("Sentinel @{$connectionName} gave master addr: {$r[0]}:{$r[1]}", self::LOG_KEY);
                return $r;
            } else {
                App::info("Did not get any master from sentinel @{$connectionName}", self::LOG_KEY);
            }
        }
        throw new \Exception("Master could not be discovered");
    }
}