/*
 * Magic Cloud, copyright Aista, Ltd. See the attached LICENSE file for details.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Cassandra;
using magic.node;
using magic.node.contracts;
using magic.node.extensions;

namespace magic.lambda.cql.helpers
{
    /*
     * Helper class for common methods.
     */
    internal static class Utilities
    {
        static readonly ConcurrentDictionary<string, Cluster> _clusters = new System.Collections.Concurrent.ConcurrentDictionary<string, Cluster>();

        /*
         * Creates a ScyllaDB session and returns to caller.
         */
        internal static ISession CreateSession(string cluster, string keyspace)
        {
            return _clusters.GetOrAdd(cluster, (key) =>
            {
                return Cluster.Builder()
                    .AddContactPoints(key)
                    .Build();
            }).Connect(keyspace);
        }
    }
}