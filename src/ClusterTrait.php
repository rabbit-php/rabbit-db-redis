<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;

/**
 * Trait ClusterTrait
 * @package rabbit\db\redis
 */
trait ClusterTrait
{
    /** @var bool */
    protected bool $cluster = false;

    /**
     * @return bool
     */
    public function getCluster(): bool
    {
        return $this->cluster;
    }
}