package it.unimi.di.law.bubing.frontier;

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

import it.unimi.di.law.bubing.RuntimeConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A thread that continuously dequeues a {@link VisitState} from the {@link Frontier#done} queue and 
 *  {@link Workbench#release(VisitState) releases} it to the {@link Workbench}.
 *  
 * @see TodoThread
 */
public final class DoneThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger( DoneThread.class );

	/** A reference to the frontier. */
	private final Frontier frontier;
	
	/** A {@link DoneThread} for the given {@link Frontier}.
	 * 
	 * @param frontier the frontier.
	 */
	public DoneThread( final Frontier frontier ) {
		this.frontier = frontier;
		setName( this.getClass().getSimpleName() );
		setPriority( Thread.MAX_PRIORITY );
	}
	
	@Override
	public void run() {
		try {
			final RuntimeConfiguration rc = frontier.rc;
			main: while( ! rc.stopping ) {
				VisitState visitState;
				for ( int i = 0; ( visitState = frontier.done.poll() ) == null; i++ ) {
					if ( rc.stopping ) break main;
					Thread.sleep( 1 << Math.min( i, 10 ) );
				}			

				// We do not schedule for refill purged visit states
				if ( visitState.nextFetch != Long.MAX_VALUE && frontier.virtualizer.count( visitState ) > 0 && visitState.isEmpty() ) frontier.refill.add( visitState );
				frontier.workbench.release( visitState );
			}
		}
		catch ( Throwable t ) {
			LOGGER.error( "Unexpected exception", t );
		}
		LOGGER.info( "Completed" );
	}
}
