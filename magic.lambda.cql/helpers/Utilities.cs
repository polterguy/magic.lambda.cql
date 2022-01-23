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
         * Returns connection settings for Cluster and keyspace to caller given the  specified node.
         */
        internal static (string Cluster, string KeySpace) GetConnectionSettings(IConfiguration configuration, Node node)
        {
            var value = node.GetEx<string>();
            if (value.StartsWith("[") && value.EndsWith("]"))
            {
                var splits = value.Substring(1, value.Length - 2).Split('|');
                if (splits.Count() != 2)
                    throw new HyperlambdaException($"I don't understand how to connect to a CQL database using '{value}'");
                return (configuration[$"magic:cql:{splits[0]}:host"], splits[1]);
            }
            else
            {
                return ("generic", value);
            }
        }

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