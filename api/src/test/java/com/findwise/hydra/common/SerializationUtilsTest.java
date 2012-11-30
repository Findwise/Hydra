package com.findwise.hydra.common;

import java.util.Date;

import junit.framework.Assert;

import org.junit.Test;

public class SerializationUtilsTest {

	@Test
	public void testDates() throws Exception {
		Assert.assertEquals(Date.class, SerializationUtils.toObject(SerializationUtils.toJson(new Date())).getClass());
		
		//SerializationUtils.toJson(SerializationUtils.fromJson("{_action=null, _id={_inc=-1320293295, _new=false, _time=1354236890, _machine=1951282283}, contents={id=xdIJbiV1BlSD, f=JsNOwXa1, d=DQkP, e=Umj9n, b=p5d4wNOtPbs2QC5VQ9, c=Fize, a=0SVtbmBhlPbOw, in=e, z=r1I0vOMlEs, y=f1mkaEna1L2iVdg, x=15}, metadata={touched={insertStage=Fri Nov 30 01:54:50 CET 2012, sleepy1=Fri Nov 30 01:56:42 CET 2012, sleepy3=Fri Nov 30 01:56:42 CET 2012, sleepy4=Fri Nov 30 01:56:41 CET 2012, sleepy5=Fri Nov 30 01:56:42 CET 2012, sleepy6=Fri Nov 30 01:56:41 CET 2012, sleepy7=Fri Nov 30 01:56:42 CET 2012, sleepy8=Fri Nov 30 01:56:42 CET 2012, sleepy9=Fri Nov 30 01:56:32 CET 2012}, fetched={sleepy1=Fri Nov 30 01:56:42 CET 2012, sleepy0=Fri Nov 30 01:56:42 CET 2012, sleepy3=Fri Nov 30 01:56:42 CET 2012, sleepy4=Fri Nov 30 01:56:41 CET 2012, sleepy5=Fri Nov 30 01:56:41 CET 2012, sleepy6=Fri Nov 30 01:56:41 CET 2012, sleepy7=Fri Nov 30 01:56:42 CET 2012, sleepy8=Fri Nov 30 01:56:42 CET 2012, sleepy9=Fri Nov 30 01:56:32 CET 2012}}}"));
	}

}
