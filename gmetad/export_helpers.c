#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <rrd.h>
#include <gmetad.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/poll.h>
#include <arpa/inet.h>

#ifdef WITH_MEMCACHED
#include <libmemcached-1.0/memcached.h>
#include <libmemcachedutil-1.0/util.h>
#endif /* WITH_MEMCACHED */

#ifdef WITH_RIEMANN
#include <riemann/event.h>
#include <riemann/message.h>
#endif /* WITH_RIEMANN */

#include "export_helpers.h"

#define PATHSIZE 4096
extern gmetad_config_t gmetad_config;

g_udp_socket *carbon_udp_socket;
pthread_mutex_t  carbon_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifdef WITH_RIEMANN
pthread_mutex_t  riemann_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif /* WITH_RIEMANN */

g_udp_socket*
init_carbon_udp_socket (const char *hostname, uint16_t port)
{
   int sockfd;
   g_udp_socket* s;
   struct sockaddr_in *sa_in;
   struct hostent *hostinfo;

   sockfd = socket(AF_INET, SOCK_DGRAM, 0);
   if (sockfd < 0)
      {
         err_msg("create socket (client): %s", strerror(errno));
         return NULL;
      }

   s = malloc( sizeof( g_udp_socket ) );
   memset( s, 0, sizeof( g_udp_socket ));
   s->sockfd = sockfd;
   s->ref_count = 1;

   /* Set up address and port for connection */
   sa_in = (struct sockaddr_in*) &s->sa;
   sa_in->sin_family = AF_INET;
   sa_in->sin_port = htons (port);
   hostinfo = gethostbyname (hostname);
   sa_in->sin_addr = *(struct in_addr *) hostinfo->h_addr;

   return s;
}

void init_sockaddr (struct sockaddr_in *name, const char *hostname, uint16_t port)
{
  struct hostent *hostinfo;

  name->sin_family = AF_INET;
  name->sin_port = htons (port);
  hostinfo = gethostbyname (hostname);
  if (hostinfo == NULL)
    {
      fprintf (stderr, "Unknown host %s.\n", hostname);
      exit (EXIT_FAILURE);
    }
  name->sin_addr = *(struct in_addr *) hostinfo->h_addr;
}

static int
push_data_to_carbon( char *graphite_msg)
{

  if (!strcmp(gmetad_config.carbon_protocol, "tcp"))
    {

      int port;
      int carbon_socket;
      struct sockaddr_in server;
      int carbon_timeout ;
      int nbytes;
      struct pollfd carbon_struct_poll;
      int poll_rval;
      int fl;

      if (gmetad_config.carbon_timeout)
          carbon_timeout=gmetad_config.carbon_timeout;
      else
          carbon_timeout = 500;

      if (gmetad_config.carbon_port)
         port=gmetad_config.carbon_port;
      else
         port=2003;

      debug_msg("Carbon Proxy:: sending \'%s\' to %s", graphite_msg, gmetad_config.carbon_server);

          /* Create a socket. */
      carbon_socket = socket (PF_INET, SOCK_STREAM, 0);
      if (carbon_socket < 0)
        {
          err_msg("socket (client): %s", strerror(errno));
          close (carbon_socket);
          return EXIT_FAILURE;
        }

      /* Set the socket to not block */
      fl = fcntl(carbon_socket,F_GETFL,0);
      fcntl(carbon_socket,F_SETFL,fl | O_NONBLOCK);

      /* Connect to the server. */
      init_sockaddr (&server, gmetad_config.carbon_server, port);
      connect (carbon_socket, (struct sockaddr *) &server, sizeof (server));

      /* Start Poll */
       carbon_struct_poll.fd=carbon_socket;
       carbon_struct_poll.events = POLLOUT;
       poll_rval = poll( &carbon_struct_poll, 1, carbon_timeout ); // default timeout .5s

      /* Send data to the server when the socket becomes ready */
      if( poll_rval < 0 ) {
        debug_msg("carbon proxy:: poll() error");
      } else if ( poll_rval == 0 ) {
        debug_msg("carbon proxy:: Timeout connecting to %s",gmetad_config.carbon_server);
      } else {
        if( carbon_struct_poll.revents & POLLOUT ) {
          /* Ready to send data to the server. */
          debug_msg("carbon proxy:: %s is ready to receive",gmetad_config.carbon_server);
          nbytes = write (carbon_socket, graphite_msg, strlen(graphite_msg) + 1);
          if (nbytes < 0) {
            err_msg("write: %s", strerror(errno));
            close(carbon_socket);
            return EXIT_FAILURE;
          }
        } else if ( carbon_struct_poll.revents & POLLHUP ) {
          debug_msg("carbon proxy:: Recvd an RST from %s during transmission",gmetad_config.carbon_server);
         close(carbon_socket);
         return EXIT_FAILURE;
        } else if ( carbon_struct_poll.revents & POLLERR ) {
          debug_msg("carbon proxy:: Recvd an POLLERR from %s during transmission",gmetad_config.carbon_server);
          close(carbon_socket);
          return EXIT_FAILURE;
        }
      }
      close (carbon_socket);
      return EXIT_SUCCESS;

  } else {

      int nbytes;

      pthread_mutex_lock( &carbon_mutex );
      nbytes = sendto (carbon_udp_socket->sockfd, graphite_msg, strlen(graphite_msg), 0,
                         (struct sockaddr_in*)&carbon_udp_socket->sa, sizeof (struct sockaddr_in));
      pthread_mutex_unlock( &carbon_mutex );

      if (nbytes != strlen(graphite_msg))
      {
             err_msg("sendto socket (client): %s", strerror(errno));
             return EXIT_FAILURE;
      }
      return EXIT_SUCCESS;
  }
}

#ifdef WITH_MEMCACHED
#define MEMCACHED_MAX_KEY_LENGTH 250 /* Maximum allowed by memcached */
int
write_data_to_memcached ( const char *cluster, const char *host, const char *metric, 
                    const char *sum, unsigned int process_time, unsigned int expiry )
{
   time_t expiry_time;
   char s_path[MEMCACHED_MAX_KEY_LENGTH];
   if (strlen(cluster) + strlen(host) + strlen(metric) + 3 > MEMCACHED_MAX_KEY_LENGTH) {
      debug_msg("Cluster + host + metric + 3 > %d", MEMCACHED_MAX_KEY_LENGTH);
      return EXIT_FAILURE;
   }
   sprintf(s_path, "%s/%s/%s", cluster, host, metric);

   if (expiry != 0) {
      expiry_time = expiry;
   } else {
      expiry_time = (time_t) 0;
   }

   memcached_return_t rc;
   memcached_st *memc = memcached_pool_pop(memcached_connection_pool, false, &rc);
   if (rc != MEMCACHED_SUCCESS) {
      debug_msg("Unable to retrieve a memcached connection from the pool");
      return EXIT_FAILURE;
   }
   rc = memcached_set(memc, s_path, strlen(s_path), sum, strlen(sum), expiry_time, (uint32_t)0);
   if (rc != MEMCACHED_SUCCESS) {
      debug_msg("Unable to push %s value %s to the memcached server(s) - %s", s_path, sum, memcached_strerror(memc, rc));
      memcached_pool_push(memcached_connection_pool, memc);
      return EXIT_FAILURE;
   } else {
      debug_msg("Pushed %s value %s to the memcached server(s)", s_path, sum);
      memcached_pool_push(memcached_connection_pool, memc);
      return EXIT_SUCCESS;
   }
}
#endif /* WITH_MEMCACHED */

/* This function replaces the macros (%s, %m etc..) in the given graphite_string*/
char *
path_macro_replace(char *path, graphite_path_macro *patrn)
{
	char *final=malloc(PATHSIZE); //heap-side so we can pass it back
	char path_cp[PATHSIZE]; //copy of path so we can clobber it
	char *prefix; 
	char *suffix;
	char *offset;     

	strncpy(final, path, PATHSIZE);
	strncpy(path_cp, path, PATHSIZE);
	for(int i=0; patrn[i].torepl != 0; i++){
		while((offset = strstr(path_cp, patrn[i].torepl)))
		{
			prefix=path_cp; //pointer to the beginning of path_cp (for clarity)
			suffix=offset+(strlen(patrn[i].torepl));// get a pointer to after patrn 
			*offset='\0'; // split the path_cp string at the first byte of patrn
			snprintf(final,PATHSIZE,"%s%s%s",prefix,patrn[i].replwith,suffix); //build a new final from the pieces
			strncpy(path_cp, final,PATHSIZE); 
		} 
	}
	return final;
}

int
write_data_to_carbon ( const char *source, const char *host, const char *metric, 
                    const char *sum, unsigned int process_time )
{

	int hostlen=strlen(host);
	char hostcp[hostlen+1]; 
	int sourcelen=strlen(source);		
	char sourcecp[sourcelen+1];
	int metriclen=strlen(metric);		
	char metriccp[metriclen+1];
	char s_process_time[15];
   char graphite_msg[ PATHSIZE + 1 ];
   int i;
                                                                                                                                                                                               
	/*  if process_time is undefined, we set it to the current time */
	if (!process_time) process_time = time(0);
	sprintf(s_process_time, "%u", process_time);

   /* prepend everything with graphite_prefix if it's set */
   if (gmetad_config.graphite_prefix != NULL && strlen(gmetad_config.graphite_prefix) > 1) {
     strncpy(graphite_msg, gmetad_config.graphite_prefix, PATHSIZE);
   }

	/*prep the source name*/
   if (source) {

		/* find and replace space for _ in the sourcename*/
		for(i=0; i<=sourcelen; i++){
			if ( source[i] == ' ') {
	  			sourcecp[i]='_';
			}else{
	  			sourcecp[i]=source[i];
			}
      }
		sourcecp[i+1]=0;
      }


   /* prep the host name*/
   if (host) {
		/* find and replace . for _ in the hostname*/
		for(i=0; i<=hostlen; i++){
         if ( host[i] == '.') {
           hostcp[i]='_';
         }else{
           hostcp[i]=host[i];
         }
      }
		hostcp[i+1]=0;
      i = strlen(graphite_msg);
      if(gmetad_config.case_sensitive_hostnames == 0) {
         /* Convert the hostname to lowercase */
         for( ; graphite_msg[i] != 0; i++)
            graphite_msg[i] = tolower(graphite_msg[i]);
      }
   }

	/*if graphite_path is set, then process it*/
   if (gmetad_config.graphite_path != NULL && strlen(gmetad_config.graphite_path) > 1) {
		graphite_path_macro patrn[4]; //macros we need to replace in graphite_path
   	char graphite_path_cp[ PATHSIZE + 1 ]; //copy of graphite_path
   	char *graphite_path_ptr; //a pointer to catch returns from path_macro_replace()
   	strncpy(graphite_path_cp,gmetad_config.graphite_path,PATHSIZE);

		patrn[0].torepl="%s";
		patrn[0].replwith=sourcecp;
		patrn[1].torepl="%h";
		patrn[1].replwith=hostcp;
		patrn[2].torepl="%m";
		patrn[2].replwith=metriccp; 
		patrn[3].torepl='\0'; //explicitly cap the array

		graphite_path_ptr=path_macro_replace(graphite_path_cp, patrn); 
		strncpy(graphite_path_cp,graphite_path_ptr,PATHSIZE);
		free(graphite_path_ptr);//malloc'd in path_macro_replace()

		/* add the graphite_path to graphite_msg (with a dot first if prefix exists) */
   	if (gmetad_config.graphite_prefix != NULL && strlen(gmetad_config.graphite_prefix) > 1) {
   		strncat(graphite_msg, ".", PATHSIZE-strlen(graphite_msg));
   		strncat(graphite_msg, graphite_path_cp, PATHSIZE-strlen(graphite_msg));
   	} else {
     		strncpy(graphite_msg, sourcecp, PATHSIZE);
		}
       
   }else{ /* no graphite_path specified, so do things the old way */
   	if (gmetad_config.graphite_prefix != NULL && strlen(gmetad_config.graphite_prefix) > 1) {
			strncat(graphite_msg, ".", PATHSIZE-strlen(graphite_msg));
   	   strncat(graphite_msg, sourcecp, PATHSIZE-strlen(graphite_msg));
   	} else {
     		strncpy(graphite_msg, sourcecp, PATHSIZE);
   	}
      strncat(graphite_msg, ".", PATHSIZE-strlen(graphite_msg));
      strncat(graphite_msg, hostcp, PATHSIZE-strlen(graphite_msg));
   	strncat(graphite_msg, ".", PATHSIZE-strlen(graphite_msg));
   	strncat(graphite_msg, metric, PATHSIZE-strlen(graphite_msg));
  	}

	/* finish off with the value and date (space separated) */
	strncat(graphite_msg, " ", PATHSIZE-strlen(graphite_msg));
  	strncat(graphite_msg, sum, PATHSIZE-strlen(graphite_msg));
  	strncat(graphite_msg, " ", PATHSIZE-strlen(graphite_msg));
  	strncat(graphite_msg, s_process_time, PATHSIZE-strlen(graphite_msg));
  	strncat(graphite_msg, "\n", PATHSIZE-strlen(graphite_msg));

	graphite_msg[strlen(graphite_msg)+1] = 0;
   return push_data_to_carbon( graphite_msg );
}

#ifdef WITH_RIEMANN
#define STATIC_ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

char **strtoknize(const char *str, size_t slen, const char *dlmt, int dlen, int  *n_toks)
{
        char *toks = alloca(slen);
        char *save_ptr = NULL;
        char *ptr = NULL;
        int i;
        char **buffer = NULL;

        strncpy(toks, str, slen);

        if (!strcmp(&toks[slen - dlen], dlmt)) { /* remove trailing delimiters */
                toks[slen - dlen] = '\0';
        }

        i = 0;
        ptr = strtok_r(toks, dlmt, &save_ptr);
        while (ptr) {
                i++;
                buffer = realloc(buffer, sizeof (char **) * i);
                assert(buffer);
                buffer[i - 1] = strdup(ptr);
                assert(buffer[i - 1]);
                ptr = strtok_r(NULL, dlmt, &save_ptr);
        }

        if (n_toks)
                *n_toks = i;

        return buffer;
}

int
send_data_to_riemann (const char *grid, const char *cluster, const char *host, const char *metric, const char *value,
                      const char *state, unsigned int localtime, const char *tags_str, unsigned int ttl)
{
   riemann_message_t msg = RIEMANN_MSG_INIT;
   riemann_message_t *resp = NULL;
   riemann_event_t **events;

   debug_msg("grid = %s, cluster = %s, host = %s, metric = %s, value = %s, state = %s, localtime = %d, tags = %s, ttl = %d",
              grid, cluster, host, metric, value, state, localtime, tags_str, ttl);

   int error;

   events = riemann_event_alloc_events(1);
   events[0] = riemann_event_alloc_event();
   riemann_event_init(events[0]);

   riemann_event_set_host(events[0], host);
   riemann_event_set_service(events[0], metric);
   if (value)
      riemann_event_set_metric_d(events[0], (double) strtod(value, (char**) NULL));
   if (state)
      riemann_event_set_state(events[0], state);
   if (localtime)
      riemann_event_set_time(events[0], localtime);

   char **tags = NULL;
   int n_tags;
   tags = strtoknize(tags_str, strlen(tags_str) + 1, ",", 1, &n_tags);
   riemann_event_set_tags(events[0], (const char **)tags, n_tags);

   riemann_attribute_pairs_t *pairs = NULL;
   int n_attrs;

   do {
       char **attrs = NULL;
       char **pair = NULL;
       int i;

       char attributes[512];
       sprintf(attributes, "%s,grid=%s,cluster=%s,resource=%s", gmetad_config.riemann_attributes, grid, cluster, host);
       attrs = strtoknize(attributes, strlen(attributes) + 1, ",", 1, &n_attrs);
       pairs = malloc(sizeof (riemann_attribute_pairs_t) * n_attrs);
       for (i = 0; i < n_attrs; i++) {
           pair = strtoknize(attrs[i], strlen(attrs[i]) + 1, "=", 1, NULL);
           pairs[i].key = strdup(pair[0]);
           pairs[i].value = strdup(pair[1]);
           free(pair[0]);
           free(pair[1]);
           free(pair);
       }
   } while (0);
   riemann_event_set_attributes(events[0], pairs, n_attrs);

   riemann_event_set_ttl(events[0], ttl);
   riemann_message_set_events(&msg, events, 1);

   char buffer[BUFSIZ];
   static char *format = "%T,%h,%s,%S,%mf,%md,%mi,%t,%G,%a";
   error = riemann_event_strfevent(buffer, BUFSIZ, format, events[0]);
   debug_msg(buffer);

   pthread_mutex_lock( &riemann_mutex );
   error = riemann_client_send_message(&riemann_cli, &msg, 0, NULL);
   pthread_mutex_unlock( &riemann_mutex );

   if (error)
      err_msg("Can't send message: %s", strerror(errno));

   if (!strcmp(gmetad_config.riemann_protocol, "tcp"))
     {
       resp = riemann_client_recv_message(&riemann_cli, 0, NULL);
       if (!resp)
         {
           err_msg("riemann_client_recv_message() got no response\n");
           riemann_events_free(events, 1);
           return EXIT_FAILURE;
         }
       if (!resp->ok)
         {
           err_msg("riemann message error: %s\n", resp->error);
           riemann_events_free(events, 1);
           return EXIT_FAILURE;
         }
       for (int i = 0; i < resp->n_events; i++)
         {
           char buffer[BUFSIZ];
           static char *format = "%T,%h,%s,%S,%mf,%md,%mi,%t,%G,%a";
           error = riemann_event_strfevent(buffer, BUFSIZ, format, resp->events[i]);
           if (error)
             {
               err_msg("riemann_event_strfevent() error");
               return EXIT_FAILURE;
            }
            debug_msg(buffer);
        }
        riemann_message_free(resp);
     }
   riemann_events_free(events, 1);
   return EXIT_SUCCESS;

}
#endif /* WITH_RIEMANN */
