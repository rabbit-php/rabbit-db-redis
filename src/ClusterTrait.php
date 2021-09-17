<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;

trait ClusterTrait
{
    protected bool $cluster = false;
    
    public function getCluster(): bool
    {
        return $this->cluster;
    }
}