package txn;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.DecimalFormat;

import org.apache.log4j.Logger;

// Also depends on class NtpMessage

/**
 * The local clock offset calculation is implemented according to the
 * SNTP algorithm specified in RFC 2030.
 * 
 * The code is based on the Java implementation of an SNTP client
 * copyrighted under the terms of the GPL by Adam Buckley in 2004.
 *
 * Lots of information at the home page of David L. Mills:
 * http://www.eecis.udel.edu/~mills
 */

/*
      Timestamp Name          ID   When Generated
      ------------------------------------------------------------
      Originate Timestamp     T1   time request sent by client
      Receive Timestamp       T2   time request received by server
      Transmit Timestamp      T3   time reply sent by server
      Destination Timestamp   T4   time reply received by client

   The roundtrip delay d and local clock offset t are defined as follows:

      delay = (T4 - T1) - (T3 - T2)   offset = ((T2 - T1) + (T3 - T4)) / 2

 */

/*
  RFS 1305 timestamp format: seconds relative to 0h on 1 January 1900.
 */

public class SntpClient {

	private static final int PORT = 123;  // SNTP UDP port
	final String serverName = "us.pool.ntp.org";
	DatagramSocket socket = null;
	InetAddress address = null;
	final byte[] buffer = new NtpMessage().toByteArray();


	public long printNTPTime(Logger logger) {
		try {
			socket = new DatagramSocket();
		} catch (SocketException e) {e.printStackTrace();}
		try {
			address = InetAddress.getByName(serverName);
		} catch (UnknownHostException e) {e.printStackTrace();}
		DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, PORT);
		try {
			socket.send(packet);
		} catch (IOException e) {e.printStackTrace();}

		packet = new DatagramPacket(buffer, buffer.length);
		try {
			socket.receive(packet);
		} catch (IOException e) {e.printStackTrace();}

		// Immediately record the incoming timestamp
		final double destinationTimestamp = NtpMessage.now ();
		final NtpMessage msg = new NtpMessage(packet.getData());
		socket.close();

		/* Presumably, msg.orginateTimestamp unchanged by server. */

		// Formula for delay according to the RFC2030 errata
		final double roundTripDelay =
				(destinationTimestamp - msg.originateTimestamp) -
				(msg.transmitTimestamp - msg.receiveTimestamp);

		// The amount the server is ahead of the client
		final double localClockOffset =
				((msg.receiveTimestamp - msg.originateTimestamp) +
						(msg.transmitTimestamp - destinationTimestamp)) / 2;


		// Display response
		System.out.format ("NTP server: %s%n", address);
		System.out.printf ("Round-trip delay:   %+9.2f ms%n", 1000*roundTripDelay);
		System.out.printf ("Local clock offset: %+9.2f ms%n", 1000*localClockOffset);

		final long now = System.currentTimeMillis();  // milliseconds 1x10e-3 seconds
		final long cor = now + Math.round (1000.0*localClockOffset);
		System.out.printf ("Local time:      %1$ta, %1$td %1$tb %1$tY, %1$tI:%1$tm:%1$tS.%1$tL %1$tp %1$tZ%n", now);
		System.out.printf ("Corrected time:  %1$ta, %1$td %1$tb %1$tY, %1$tI:%1$tm:%1$tS.%1$tL %1$tp %1$tZ%n", cor);
		logger.info("NTP time now: "+now);
		logger.info("NTP time corrected: "+cor);		
		return cor;
	}
}
