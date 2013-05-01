#include "ganglia.h"

#ifdef WITH_MEMCACHED
#include <libmemcached-1.0/memcached.h>
#include <libmemcachedutil-1.0/util.h>
#endif /* WITH_MEMCACHED */

/* Tracks the macros we need to interpret in the graphite_path */
typedef struct 
{
	char *torepl;
	char *replwith;
}
graphite_path_macro;

#ifdef WITH_MEMCACHED
int
write_data_to_memcached ( const char *cluster, const char *host, const char *metric, 
                    const char *sum, unsigned int process_time, unsigned int expiry );
#endif /* WITH_MEMCACHED */

g_udp_socket*
init_carbon_udp_socket (const char *hostname, uint16_t port);
int
write_data_to_carbon ( const char *source, const char *host, const char *metric, 
                    const char *sum, unsigned int process_time);

#ifdef WITH_RIEMANN
int
send_data_to_riemann (const char *grid,          /* grid        => grid */
                      const char *cluster,       /* cluster     => cluster */
                      const char *host,          /* host        => host */
                      const char *metric,        /* metric      => service */
                      const char *value,         /* value       => metric */
                      const char *state,         /* string      => state */
                      unsigned int localtime,    /* localtime   => time */
                      const char *tags,          /* tags        => tags */
                      unsigned int ttl           /* dmax        => ttl */
                      );
#endif /* WITH_RIEMANN */
