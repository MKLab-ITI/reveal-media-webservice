package it.unimi.di.law.bubing.util;

/*		 
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.io.FastBufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

//RELEASE-STATUS: DIST

/** A class providing static methods to parse <samp>robots.txt</samp> into arrays of char arrays and
 * handle robot filtering. */

public class URLRespectsRobots {
	private final static Logger LOGGER = LoggerFactory.getLogger( URLRespectsRobots.class );
	
	/** The maximum number of robots entries returned by {@link #toString()}. */
	public static final int MAX_TO_STRING_ROBOTS = 30;
	
	private URLRespectsRobots() {}
	
	/** A singleton empty robots filter. */
	public final static char[][] EMPTY_ROBOTS_FILTER = new char[ 0 ][];
	
	public static char[][] toSortedPrefixFreeCharArrays( final Set<String> set ) {
		final int size = set.size();
		final String[] s = set.toArray( new String[ size ] );
		Arrays.sort( s );
		int j = 0;
		if ( size != 0 ) {
			for( int i = 1; i < size; i++ ) if ( ! s[ i ].startsWith( s[ j ] ) )  s[ ++j ] = s[ i ];
			++j;
		}
		final char[][] result = new char[ j ][];
		for( int i = 0; i < j; i++ ) result[ i ] = s[ i ].toCharArray();
		return result;
	}
	
	/** Parses the argument as if it were the content of a <samp>robots.txt</samp> file,
	 * and returns a sorted array of prefixes of URLs that the agent should not follow.
	 *
	 * @param content the content of the  <samp>robots.txt</samp> file.
	 * @param userAgent the string representing the user agent of interest.
	 * @return an array of character arrays, which are prefixes of the URLs not to follow, in sorted order.
	 */
	public static char[][] parseRobotsReader( final Reader content, final String userAgent ) throws IOException {
		/* The set of disallowed paths specifically aimed at userAgent. */
		Set<String> set = new ObjectOpenHashSet<String>();
		/* The set of disallowed paths specifically aimed at *. */
		Set<String> setStar = new ObjectOpenHashSet<String>();
		/* True if the currently examined record is targetted to us. */
		boolean doesMatter = false;
		/* True if we have seen a section targetted to our agent. */
		boolean specific = false;
		/* True if we have seen a section targetted to *. */
		boolean generic = false;
		/* True if we are in a star section. */
		boolean starSection = false;

		StreamTokenizer st = new StreamTokenizer( new FastBufferedReader( content ) );
		int token;

		st.resetSyntax();
		st.eolIsSignificant( true ); // We need EOLs to separate records
		st.wordChars( 33, 255 ); // All characters may appear
		st.whitespaceChars( 0, 32 );
		st.ordinaryChar( '#' ); // We must manually simulate comments 8^(
		st.lowerCaseMode( false );
		
		while ( true ) {
			int lineFirstToken = st.nextToken();
			if ( lineFirstToken == StreamTokenizer.TT_EOF ) break;
			
 			switch ( lineFirstToken ) {
 				// Blank line: a new block is starting
				case StreamTokenizer.TT_EOL: 
					doesMatter = false; 
					break;
					
				// Comment or number: ignore until the end of line
				case StreamTokenizer.TT_NUMBER: 
				case '#': 
					do {
						token = st.nextToken();
					} while ( token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF );
					break;
					
				// A string
				case StreamTokenizer.TT_WORD:
					if ( st.sval.equalsIgnoreCase( "user-agent:" ) ) {
						token = st.nextToken();
						if ( token == StreamTokenizer.TT_WORD )
							if ( StringUtils.startsWithIgnoreCase( userAgent, st.sval ) ) { 
								doesMatter = true;
								specific = true;
								starSection = false;
							}
							else if ( st.sval.equals( "*" ) ) {
								starSection = true;
								generic = true;
							} else starSection = false;
						// Ignore the rest of the line
						while ( token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF ) 
							token = st.nextToken();
					} else if ( st.sval.equalsIgnoreCase( "disallow:" ) ) {
						token = st.nextToken();
						//System.out.println( st.sval + " " + starSection + " " + set + " " + setStar );
						if ( token == StreamTokenizer.TT_EOL ) { 
							if ( doesMatter ) set.clear();
							else if ( starSection ) setStar.clear();
						} else if ( token == StreamTokenizer.TT_WORD ) {
							String disallowed = st.sval;
							if ( disallowed.endsWith( "*" ) ) disallowed = disallowed.substring( 0, disallowed.length()-1 ); // Someone (erroneously) uses * to denote any suffix
							if ( doesMatter ) set.add( disallowed );
							else if ( starSection ) setStar.add( disallowed );
						}
						// Ignore the rest of the line
						while ( token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF ) 
							token = st.nextToken();						
					} else if ( LOGGER.isTraceEnabled() ) LOGGER.trace( "Line first token {} ununderstandable in robots.txt", st.sval );
					break;
					
				// Something else: a syntax error	
				default: 
					if ( LOGGER.isTraceEnabled() ) LOGGER.trace( "Found unknown token type {} in robots.txt", Integer.valueOf( lineFirstToken ) );
			}
		}
		
		// TODO: can we avoid the three-legged return?
		if ( specific ) return toSortedPrefixFreeCharArrays( set ); // Some instructions specific to us
		if ( ! specific && generic ) return toSortedPrefixFreeCharArrays( setStar ); // No specific instruction, but some generic ones
		return toSortedPrefixFreeCharArrays( set );
	}
	
	/** Parses a <samp>robots.txt</samp> file contained in a {@link FetchData} and
	 * returns the corresponding filter as an array of sorted prefixes. HTTP statuses
	 * different from 2xx are {@linkplain Logger#warn(String) logged}. HTTP statuses of class 4xx
	 * generate an empty filter. HTTP statuses 2xx/3xx cause the tentative parsing of the
	 * request content. In the remaining cases we return {@code null}.
	 * 
	 * @param robotsResponse the response containing <samp>robots.txt</samp>. 
	 * @param userAgent the string representing the user agent of interest.
	 * @return an array of character arrays, which are prefixes of the URLs not to follow, in sorted order, 
	 * or {@code null}
	 */
	public static char[][] parseRobotsResponse( final URIResponse robotsResponse, final String userAgent ) throws IOException {
		final int status = robotsResponse.response().getStatusLine().getStatusCode();
		if ( status / 100 != 2 ) LOGGER.info( "Got status " + status + " while fetching robots: URL was " + robotsResponse.uri() );
		if ( status / 100 == 4 || status / 100 == 5 ) return EMPTY_ROBOTS_FILTER; // For status 4xx and 5xx, we consider everything allowed.
		if ( status / 100 != 2 && status / 100 != 3 ) return null; // For status 2xx and 3xx we parse the content. For the rest, we consider everything forbidden.
		char[][] result = parseRobotsReader( new InputStreamReader( robotsResponse.response().getEntity().getContent(), Charsets.ISO_8859_1 ), userAgent );
		if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Robots for {} successfully got with status {}: {}", robotsResponse.uri(), Integer.valueOf( status ), toString( result ) );
		return result;
	}
	
	private final static int compare( char[] left, String right ) {
		int l = Math.min( left.length, right.length() );
		for ( int i = 0; i < l; i++ ) {
			final int result = left[ i ] - right.charAt( i );
			if ( result != 0 ) return result;
		}
		return left.length - right.length();
	}

	private final static boolean doesNotStartsWith( final String s, final char[] prefix ) {
		if ( prefix.length > s.length() ) return true;
		for ( int i = prefix.length; i-- != 0; ) if ( s.charAt( i ) != prefix[ i ] ) return true;
		return false;
	}

	/** Checks whether a specified URL passes a specified robots filter.
	 * 
	 * @param robotsFilter a robot filter.
	 * @param url a URL to check against {@code robotsFilter}.
	 * @return true if {@code url} passes {@code robotsFilter}.
	 */
	public static boolean apply( final char[][] robotsFilter, final URI url ) {
		if ( robotsFilter.length == 0 ) return true;
		final String pathQuery = BURL.pathAndQuery( url );
		int from = 0;
		int to = robotsFilter.length - 1;
		while ( from <= to ) {
			final int mid = ( from + to ) >>> 1;
			final int cmp = compare( robotsFilter[ mid ], pathQuery );
			if ( cmp < 0 ) from = mid + 1;
			else if ( cmp > 0 ) to = mid - 1;
			else return false; // key found (unlikely, but possible)
		}
		return from == 0 ? true : doesNotStartsWith( pathQuery, robotsFilter[ from - 1 ] );
	}

	/** Prints gracefully a robot filter using at most {@value #MAX_TO_STRING_ROBOTS} prefixes.
	 * 
	 * @param robotsFilter a robots filter.
	 * @return a string describing the filter.
	 */
	public static String toString( final char[][] robotsFilter ) {
		if ( robotsFilter == null ) return "[]";
		final StringBuilder stringBuilder = new StringBuilder().append( '[' );
		final int n = Math.min( robotsFilter.length, MAX_TO_STRING_ROBOTS );
		for( int i = 0; i < n; i++ ){
			if ( i != 0 ) stringBuilder.append( "," );
			stringBuilder.append( '"' ).append( robotsFilter[ i ] ).append( '"' );
		}
		if ( n != robotsFilter.length ) stringBuilder.append( ",..." );
		return stringBuilder.append( ']' ).toString();
	}
	
	public static void main( String arg[] ) throws IOException {
		char[][] robotsResult = URLRespectsRobots.parseRobotsReader( new InputStreamReader( System.in ), arg[ 0 ] );
		for( char[] a: robotsResult ) System.err.println( new String( a ) );
	}
}
