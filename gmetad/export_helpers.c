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
#include <apr_time.h>

#ifdef WITH_MEMCACHED
#include <libmemcached-1.0/memcached.h>
#include <libmemcachedutil-1.0/util.h>
#endif /* WITH_MEMCACHED */

#ifdef WITH_RIEMANN
#include "riemann.pb-c.h"

pthread_mutex_t  riemann_mutex = PTHREAD_MUTEX_INITIALIZER;

int riemann_circuit_breaker = RIEMANN_CB_CLOSED;
apr_time_t riemann_reset_timeout = 0;
int riemann_failures = 0;
#endif /* WITH_RIEMANN */

#include "export_helpers.h"

#define PATHSIZE 4096
extern gmetad_config_t gmetad_config;

g_udp_socket *carbon_udp_socket;
pthread_mutex_t  carbon_mutex = PTHREAD_MUTEX_INITIALIZER;

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

#ifdef WITH_RIEMANN
g_udp_socket *riemann_udp_socket;
g_tcp_socket *riemann_tcp_socket;

g_udp_socket*
init_riemann_udp_socket (const char *hostname, uint16_t port)
{
   int sockfd;
   g_udp_socket* s;
   struct sockaddr_in *sa_in;
   struct hostent *hostinfo;

   sockfd = socket(AF_INET, SOCK_DGRAM, 0);
   if (sockfd < 0)
      return NULL;

   s = malloc( sizeof( g_udp_socket ) );
   memset( s, 0, sizeof( g_udp_socket ));
   s->sockfd = sockfd;
   s->ref_count = 1;

   /* Set up address and port for connection */
   sa_in = (struct sockaddr_in*) &s->sa;
   sa_in->sin_family = AF_INET;
   sa_in->sin_port = htons (port);
   hostinfo = gethostbyname (hostname);
   if (!hostinfo)
      err_quit("Unknown host %s", hostname);
   sa_in->sin_addr = *(struct in_addr *) hostinfo->h_addr;

   return s;
}

g_tcp_socket*
init_riemann_tcp_socket (const char *hostname, uint16_t port)
{
   int sockfd;
   g_tcp_socket* s;
   struct sockaddr_in *sa_in;
   struct hostent *hostinfo;
   int rv;

   sockfd = socket (AF_INET, SOCK_STREAM, 0);
   if (sockfd < 0)
      return NULL;

   s = malloc( sizeof( g_tcp_socket ) );
   memset( s, 0, sizeof( g_tcp_socket ));
   s->sockfd = sockfd;
   s->ref_count = 1;

   /* Set up address and port for connection */
   sa_in = (struct sockaddr_in*) &s->sa;
   sa_in->sin_family = AF_INET;
   sa_in->sin_port = htons (port);
   hostinfo = gethostbyname (hostname);
   if (!hostinfo)
      err_quit("Unknown host %s", hostname);
   sa_in->sin_addr = *(struct in_addr *) hostinfo->h_addr;

   if (riemann_tcp_socket)
      close (riemann_tcp_socket->sockfd);

   // set to non-blocking
   long flags = fcntl (sockfd, F_GETFL, 0);
   fcntl (sockfd, F_SETFL, flags | O_NONBLOCK);

   connect (sockfd, &s->sa, sizeof(s->sa));

   struct pollfd pfds[0];
   pfds[0].fd = sockfd;
   pfds[0].events = POLLOUT;
   rv = poll (pfds, 1, RIEMANN_POLL_TIMEOUT);

   if (rv < 0) {
      close (sockfd);
      free (s);
      return NULL;
   } else if (rv == 0) {
      close (sockfd);
      free (s);
      return NULL;
   } else {
      if (pfds[0].revents & POLLOUT) {
         int error = 0;
         socklen_t len = sizeof(error);
         if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
            close(sockfd);
            free (s);
            return NULL;
         }
         if (error) {
            close(sockfd);
            free (s);
            return NULL;
         }
         debug_msg("[riemann] tcp socket success!");
         return s;
      } else {
         err_msg("[riemann] tcp socket fail!");
         close (sockfd);
         free (s);
         return NULL;
      }
   }
}
#endif /* WITH_RIEMANN */

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
           if (gmetad_config.case_sensitive_hostnames == 0) {
             hostcp[i] = tolower(host[i]);
           }
           else {
             hostcp[i] = host[i];
           }
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

   if (metric) {
     for(i=0; i <= metriclen; i++) {
       if (metric[i] == ' ') {
         metriccp[i] = '_';
       }
       else {
         metriccp[i] = metric[i];
       }
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
            strncpy(graphite_msg, graphite_path_cp, PATHSIZE);
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

int
tokenize (char *str, char *delim, char **tokens)
{
  char *p;
  int i = 0;

  p = strtok (str, delim);
  while (p != NULL) {
    tokens[i] = malloc (strlen (p) + 1);
    if (tokens[i])
      strcpy (tokens[i], p);
    i++;
    p = strtok (NULL, delim);
  }
  return i++;
}

int
send_data_to_riemann (const char *grid, const char *cluster, const char *host, const char *ip,
                      const char *metric, const char *value, const char *type, const char *units,
                      const char *state, unsigned int localtime, const char *tags_str,
                      const char *location, unsigned int ttl)
{
  pthread_mutex_lock( &riemann_mutex );

  int i, rval = EXIT_SUCCESS;
  char *buffer = NULL;

  debug_msg("[riemann] grid=%s, cluster=%s, host=%s, ip=%s, metric=%s, value=%s %s, type=%s, state=%s, localtime=%u, tags=%s, location=%s, ttl=%u",
            grid, cluster, host, ip, metric, value, units, type, state, localtime, tags_str, location, ttl);

  Event evt = EVENT__INIT;

  evt.host = (char *)host;
  evt.service = (char *)metric;

   if (value) {
       if (!strcmp(type, "int")) {
           evt.has_metric_sint64 = 1;
           evt.metric_sint64 = strtol(value, (char **) NULL , 10 );
       } else if (!strcmp(type, "float")) {
           evt.has_metric_d = 1;
           evt.metric_d = (double) strtod(value, (char**) NULL);
       } else {
           evt.state = (char *)value;
       }
   }
  evt.description = (char *)units;

   if (state)
      evt.state = (char *)state;

   if (localtime)
      evt.time = localtime;

  char *tags[64] = { NULL };
  buffer = strdup(tags_str);

  evt.n_tags = tokenize (buffer, ",", tags);  /* assume tags are comma-separated */
  evt.tags = tags;
  free(buffer);

  char attr_str[512];
  sprintf(attr_str, "grid=%s,cluster=%s,ip=%s,location=%s%s%s", grid, cluster, ip, location,
        gmetad_config.riemann_attributes ? "," : "",
        gmetad_config.riemann_attributes ? gmetad_config.riemann_attributes : "");

  int n_attrs;
  char *kv[64] = { NULL };
  buffer = strdup(attr_str);

  n_attrs = tokenize (buffer, ",", kv);
  free(buffer);

  Attribute **attrs;
  attrs = malloc (sizeof (Attribute *) * n_attrs);

  for (i = 0; i < n_attrs; i++) {

    char *pair[1] = { NULL };
    tokenize (kv[i], "=", pair);

    attrs[i] = malloc (sizeof (Attribute));
    attribute__init (attrs[i]);
    attrs[i]->key = pair[0];
    attrs[i]->value = pair[1];
  }
  evt.n_attributes = n_attrs;
  evt.attributes = attrs;

  evt.has_ttl = 1;
  evt.ttl = ttl;

  Msg riemann_msg = MSG__INIT;
  riemann_msg.n_events = 1;
  riemann_msg.events = malloc(sizeof (Event) * riemann_msg.n_events);
  riemann_msg.events[0] = &evt;

  debug_msg("[riemann] %zu host=%s, service=%s, state=%s, metric_f=%f, metric_d=%lf, metric_sint64=%" PRId64 ", description=%s, ttl=%f, tags(%zu), attributes(%zu)",
            evt.time, evt.host, evt.service, evt.state, evt.metric_f, evt.metric_d, evt.metric_sint64, evt.description, evt.ttl, evt.n_tags, evt.n_attributes);

  if (!strcmp (gmetad_config.riemann_protocol, "udp")) {

     int nbytes;
     unsigned len;
     void *buf;

     len = msg__get_packed_size(&riemann_msg);
     buf = malloc(len);
     msg__pack(&riemann_msg, buf);

     nbytes = sendto (riemann_udp_socket->sockfd, buf, len, 0,
                            (struct sockaddr_in*)&riemann_udp_socket->sa, sizeof (struct sockaddr_in));
     free (buf);

     if (nbytes != len)
     {
        err_msg("[riemann] ERROR %s sendto(): %s", gmetad_config.riemann_protocol, strerror(errno));
        rval = EXIT_FAILURE;
     } else {
        debug_msg("[riemann] Sent %d serialized bytes", len);
     }

  } else {
     if (riemann_circuit_breaker == RIEMANN_CB_CLOSED) {

        int nbytes;
        unsigned len;
        struct {
          uint32_t header;
          uint8_t data[0];
        } *buf;

        len = msg__get_packed_size (&riemann_msg) + sizeof (buf->header);
        buf = malloc (len);
        msg__pack (&riemann_msg, buf->data);
        buf->header = htonl (len - sizeof (buf->header));

        nbytes = send (riemann_tcp_socket->sockfd, buf, len, 0);
        free (buf);

        if (nbytes != len) {
           err_msg("[riemann] ERROR %s send(): %s", gmetad_config.riemann_protocol, strerror (errno));
           riemann_failures++;
           rval = EXIT_FAILURE;
        } else {
           debug_msg("[riemann] Sent %d serialized bytes", len);
        }

        Msg *response;
        uint32_t header, rlen;
        uint8_t *rbuf;
        ssize_t rc;

        rc = recv (riemann_tcp_socket->sockfd, &header, sizeof (header), 0);
        if (rc == 0) {  /* server closed connection */
              err_msg ("[riemann] server closed connection");
              riemann_failures = RIEMANN_MAX_FAILURES + 1;
              rval = EXIT_FAILURE;
        } else if (rc == -1) {
           if (errno != EAGAIN) {
              err_msg ("[riemann] %s", strerror(errno));
              riemann_failures++;
              rval = EXIT_FAILURE;
           } else {
              err_msg ("[riemann] EAGAIN %s", strerror(errno));
           }
        } else if (rc != sizeof (header)) {
           err_msg ("[riemann] error occurred receiving response");
           riemann_failures++;
           rval = EXIT_FAILURE;
        } else {
           rlen = ntohl (header);
           rbuf = malloc (rlen);
           recv (riemann_tcp_socket->sockfd, rbuf, rlen, 0);
           response = msg__unpack (NULL, rlen, rbuf);
           debug_msg ("[riemann] message response ok=%d", response->ok);
           free (rbuf);
        }

        if (riemann_failures > RIEMANN_MAX_FAILURES) {
           riemann_circuit_breaker = RIEMANN_CB_OPEN;
           riemann_reset_timeout = apr_time_now () + RIEMANN_RETRY_TIMEOUT * APR_USEC_PER_SEC;
           err_msg("[riemann] %d send failures exceeds maximum of %d - circuit breaker is OPEN for %d seconds",
              riemann_failures, RIEMANN_MAX_FAILURES, RIEMANN_RETRY_TIMEOUT);
       }
     }
  }

  for (i = 0; i < evt.n_attributes; i++) {
     free(attrs[i]->key);
     free(attrs[i]->value);
     free(attrs[i]);
     free(kv[i]);
  }
  free(attrs);
  for (i = 0; i < evt.n_tags; i++) {
     free(tags[i]);
  }
  free(riemann_msg.events);

  pthread_mutex_unlock( &riemann_mutex );

  return rval;
}
#endif /* WITH_RIEMANN */

