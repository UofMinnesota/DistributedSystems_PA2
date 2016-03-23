package project1;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;
import java.util.ArrayList;
import java.util.Random;

public class FilesClient {
  static boolean USE_LOCAL = false;

  public static class  NodeInfo implements Comparable<NodeInfo>{
    String address = "";
    int port = 0;
    int hash = 0;


    public int compareTo(NodeInfo N) {
        return Integer.compare(hash, N.hash);
    }


  }

  private static ArrayList<NodeName> ListOfNodes = new ArrayList<NodeName>();

  public static NodeName strToNodeName(String input)
  {
    String data[] = input.split(":");
    NodeName newNo = new NodeName(data[0].trim(),Integer.parseInt(data[1]),Integer.parseInt(data[2]));

    return newNo;
  }

  public static ArrayList<NodeName> strToNodeNameArray(String input)
  {
    ArrayList<NodeName> arrN = new ArrayList<NodeName>();
    String data[] = input.split("\\,");
    for(int c = 0; c < data.length; c++)
    {
      arrN.add(strToNodeName(data[c]));
    }
    return arrN;
  }

 public static void main(String[] args) {

   int mode = 0;
   if(args.length != 0)
   {
     mode = Integer.parseInt(args[0]);
   }
   
   if(mode == 3){
	   writeFile("ABC","ABC");
	   writeFile("XXX","XXX");
	   writeFile("TED","TED");
	   
	   readFile("TED");
	   readFile("XXX");
	   readFile("ABC");
   }
   
   if(mode == 4){
	   writeFile("ABC","ABC");
	   readFile("TED");
	   
   }
   
   if(mode <= 1)
   {
  	 writeFile("ABC","ABC");
  	 writeFile("XXX","XXX");
  	 writeFile("TED","TED");
  	 writeFile("Accoutre.txt","Accoutre.txt");
  	 writeFile("Acquit.txt","Acquit.txt");
  	 writeFile("Adverb.txt","Adverb.txt");
  	 writeFile("Adversary.txt","Adversary.txt");
  	 writeFile("Adze.txt","Adze.txt");
  	 writeFile("Aegis.txt","Aegis.txt");
  	 writeFile("Aerate.txt","Aerate.txt");
  	 writeFile("Affable.txt","Affable.txt");
  	 writeFile("Affluence.txt","Affluence.txt");
  	 writeFile("Affront.txt","Affront.txt");
  	 writeFile("Anonymous.txt","Anonymous.txt");
  	 writeFile("Another.txt","Another.txt");
  	 writeFile("Answer.txt","Answer.txt");
  	 writeFile("Backgammon.txt","Backgammon.txt");
  	 writeFile("Bail.txt","Bail.txt");
  	 writeFile("Bairn.txt","Bairn.txt");
  	 writeFile("Bald.txt","Bald.txt");
  	 writeFile("Bale.txt","Bale.txt");
  	 writeFile("Benevolent.txt","Benevolent.txt");
  	 writeFile("Benight.txt","Benight.txt");
  	 writeFile("Bestead.txt","Bestead.txt");
  	 writeFile("Bet.txt","Bet.txt");
  	 writeFile("Beverage.txt","Beverage.txt");
  	 writeFile("Bewail.txt","Bewail.txt");
  	 writeFile("Broil.txt","Broil.txt");
  	 writeFile("Broke.txt","Broke.txt");
  	 writeFile("Brunt.txt","Brunt.txt");
  	 writeFile("Buff.txt","Buff.txt");
  	 writeFile("Card.txt","Card.txt");
  	 writeFile("Career.txt","Career.txt");
  	 writeFile("Carpet.txt","Carpet.txt");
  	 writeFile("Carriage.txt","Carriage.txt");
  	 writeFile("Clamp.txt","Clamp.txt");
  	 writeFile("Clan.txt","Clan.txt");
  	 writeFile("Clandestine.txt","Clandestine.txt");
  	 writeFile("Clergy.txt","Clergy.txt");
  	 writeFile("Clever.txt","Clever.txt");
  	 writeFile("Codify.txt","Codify.txt");
  	 writeFile("Coeducation.txt","Coeducation.txt");
  	 writeFile("Coerce.txt","Coerce.txt");
  	 writeFile("Cohesion.txt","Cohesion.txt");
  	 writeFile("Coil.txt","Coil.txt");
  	 writeFile("Collar.txt","Collar.txt");
  	 writeFile("Colloquy.txt","Colloquy.txt");
  	 writeFile("Collude.txt","Collude.txt");
  	 writeFile("Concession.txt","Concession.txt");
  	 writeFile("Conch.txt","Conch.txt");
  	 writeFile("Concoct.txt","Concoct.txt");
  	 writeFile("Concomitant.txt","Concomitant.txt");
  	 writeFile("Condone.txt","Condone.txt");
  	 writeFile("Debacle.txt","Debacle.txt");
  	 writeFile("Debar.txt","Debar.txt");
  	 writeFile("Debase.txt","Debase.txt");
  	 writeFile("Decay.txt","Decay.txt");
  	 writeFile("Decease.txt","Decease.txt");
  	 writeFile("Decide.txt","Decide.txt");
  	 writeFile("Deck.txt","Deck.txt");
  	 writeFile("Declamation.txt","Declamation.txt");
  	 writeFile("Destiny.txt","Destiny.txt");
  	 writeFile("Destitute.txt","Destitute.txt");
  	 writeFile("Desultory.txt","Desultory.txt");
  	 writeFile("Detach.txt","Detach.txt");
  	 writeFile("Disable.txt","Disable.txt");
  	 writeFile("Disaffection.txt","Disaffection.txt");
  	 writeFile("Disclaim.txt","Disclaim.txt");
  	 writeFile("Discomfort.txt","Discomfort.txt");
  	 writeFile("Dryad.txt","Dryad.txt");
  	 writeFile("Ductile.txt","Ductile.txt");
  	 writeFile("Duke.txt","Duke.txt");
  	 writeFile("Duly.txt","Duly.txt");
  	 writeFile("Ebb.txt","Ebb.txt");
  	 writeFile("Ebony.txt","Ebony.txt");
  	 writeFile("Eclectic.txt","Eclectic.txt");
  	 writeFile("Ecstasy.txt","Ecstasy.txt");
  	 writeFile("Edible.txt","Edible.txt");
  	 writeFile("Eject.txt","Eject.txt");
  	 writeFile("Elaborate.txt","Elaborate.txt");
  	 writeFile("Empower.txt","Empower.txt");
  	 writeFile("Encamp.txt","Encamp.txt");
  	 writeFile("Enclave.txt","Enclave.txt");
  	 writeFile("Endear.txt","Endear.txt");
  	 writeFile("Equine.txt","Equine.txt");
  	 writeFile("Erasure.txt","Erasure.txt");
  	 writeFile("Erode.txt","Erode.txt");
  	 writeFile("Eschew.txt","Eschew.txt");
  	 writeFile("Escort.txt","Escort.txt");
  	 writeFile("Espirit.txt","Espirit.txt");
  	 writeFile("Expatiate.txt","Expatiate.txt");
  	 writeFile("Expunge.txt","Expunge.txt");
  	 writeFile("Fallible.txt","Fallible.txt");
  	 writeFile("Familiarize.txt","Familiarize.txt");
  	 writeFile("Fanfare.txt","Fanfare.txt");
  	 writeFile("Fascinate.txt","Fascinate.txt");
  	 writeFile("Flash.txt","Flash.txt");
  	 writeFile("Flaunt.txt","Flaunt.txt");
  	 writeFile("Flay.txt","Flay.txt");
  	 writeFile("Flimsy.txt","Flimsy.txt");
  	 writeFile("Flog.txt","Flog.txt");
  	 writeFile("Glutton.txt","Glutton.txt");
  	 writeFile("Goblin.txt","Goblin.txt");
  	 writeFile("Goose.txt","Goose.txt");
  	 writeFile("Guise.txt","Guise.txt");
  	 writeFile("Gullible.txt","Gullible.txt");
  	 writeFile("Gunny.txt","Gunny.txt");
  	 writeFile("Gynarchy.txt","Gynarchy.txt");
  	 writeFile("Happy.txt","Happy.txt");
  	 writeFile("Hem.txt","Hem.txt");
  	 writeFile("Henpecked.txt","Henpecked.txt");
  	 writeFile("Herbivorous.txt","Herbivorous.txt");
  	 writeFile("Heretic.txt","Heretic.txt");
  	 writeFile("Hern.txt","Hern.txt");
  	 writeFile("Horde.txt","Horde.txt");
  	 writeFile("Horseplay.txt","Horseplay.txt");
  	 writeFile("Hound.txt","Hound.txt");
  	 writeFile("Icicle.txt","Icicle.txt");
  	 writeFile("Iconoclast.txt","Iconoclast.txt");
  	 writeFile("Ideology.txt","Ideology.txt");
  	 writeFile("Ignorance.txt","Ignorance.txt");
  	 writeFile("Imperial.txt","Imperial.txt");
  	 writeFile("Impinge.txt","Impinge.txt");
  	 writeFile("Imposition.txt","Imposition.txt");
  	 writeFile("Incredulous.txt","Incredulous.txt");
  	 writeFile("Incurious.txt","Incurious.txt");
  	 writeFile("Indelicate.txt","Indelicate.txt");
  	 writeFile("Indemnify.txt","Indemnify.txt");
  	 writeFile("Inquiry.txt","Inquiry.txt");
  	 writeFile("Inquisitive.txt","Inquisitive.txt");
  	 writeFile("Inroad.txt","Inroad.txt");
  	 writeFile("Insidious.txt","Insidious.txt");
  	 writeFile("Insight.txt","Insight.txt");
  	 writeFile("Insoluble.txt","Insoluble.txt");
  	 writeFile("Involve.txt","Involve.txt");
  	 writeFile("Invulnerable.txt","Invulnerable.txt");
  	 writeFile("Jump.txt","Jump.txt");
  	 writeFile("Junction.txt","Junction.txt");
  	 writeFile("Jurisprudence.txt","Jurisprudence.txt");
  	 writeFile("Jurist.txt","Jurist.txt");
  	 writeFile("Jute.txt","Jute.txt");
  	 writeFile("Juvenile.txt","Juvenile.txt");
  	 writeFile("Lambative.txt","Lambative.txt");
  	 writeFile("Lambent.txt","Lambent.txt");
  	 writeFile("Lancet.txt","Lancet.txt");
  	 writeFile("Landgravine.txt","Landgravine.txt");
  	 writeFile("Landholder.txt","Landholder.txt");
  	 writeFile("Larynx.txt","Larynx.txt");
  	 writeFile("Lascar.txt","Lascar.txt");
  	 writeFile("Lassitude.txt","Lassitude.txt");
  	 writeFile("Litigation.txt","Litigation.txt");
  	 writeFile("Litre.txt","Litre.txt");
  	 writeFile("Loaf.txt","Loaf.txt");
  	 writeFile("Lobby.txt","Lobby.txt");
  	 writeFile("Lodestone.txt","Lodestone.txt");
  	 writeFile("Maiden.txt","Maiden.txt");
  	 writeFile("Mail.txt","Mail.txt");
  	 writeFile("Malign.txt","Malign.txt");
  	 writeFile("Misconstrue.txt","Misconstrue.txt");
  	 writeFile("Myopia.txt","Myopia.txt");
  	 writeFile("Myriad.txt","Myriad.txt");
  	 writeFile("Myth.txt","Myth.txt");
  	 writeFile("Nominee.txt","Nominee.txt");
  	 writeFile("Nonage.txt","Nonage.txt");
  	 writeFile("Offal.txt","Offal.txt");
  	 writeFile("Offence.txt","Offence.txt");
  	 writeFile("Outhouse.txt","Outhouse.txt");
  	 writeFile("Outing.txt","Outing.txt");
  	 writeFile("Outspoken.txt","Outspoken.txt");
  	 writeFile("Papyrus.txt","Papyrus.txt");
  	 writeFile("Parcel.txt","Parcel.txt");
  	 writeFile("Parish.txt","Parish.txt");
  	 writeFile("Pluck.txt","Pluck.txt");
  	 writeFile("Plumage.txt","Plumage.txt");
  	 writeFile("Pod.txt","Pod.txt");
  	 writeFile("Proclivity.txt","Proclivity.txt");
  	 writeFile("Recite.txt","Recite.txt");
  	 writeFile("Reck.txt","Reck.txt");
  	 writeFile("Slovenly.txt","Slovenly.txt");
  	 writeFile("Slush.txt","Slush.txt");
  	 writeFile("Spasmodic.txt","Spasmodic.txt");
  	 writeFile("Spite.txt","Spite.txt");
  	 writeFile("Staunch.txt","Staunch.txt");
  	 writeFile("Suavity.txt","Suavity.txt");
  	 writeFile("Talisman.txt","Talisman.txt");
  	 writeFile("Taper.txt","Taper.txt");
  	 writeFile("Tomboy.txt","Tomboy.txt");
  	 writeFile("Torchbearer.txt","Torchbearer.txt");
  	 writeFile("Trowel.txt","Trowel.txt");
  	 writeFile("Tuck.txt","Tuck.txt");
  	 writeFile("Untoward.txt","Untoward.txt");
  	 writeFile("Upright.txt","Upright.txt");
  	 writeFile("Wager.txt","Wager.txt");
  	 writeFile("Wallaby.txt","Wallaby.txt");
  	 writeFile("Wane.txt","Wane.txt");
  	 writeFile("Wilful.txt","Wilful.txt");
  	 writeFile("Winsome.txt","Winsome.txt");
  	 writeFile("Zealous.txt","Zealous.txt");
  	 writeFile("Zymotic.txt","Zymotic.txt");
   }

    if(mode == 2){
	 readFile("TED");
	 readFile("XXX");
	 readFile("ABC");
	 readFile("Accoutre.txt");
	 readFile("Acquit.txt");
	 readFile("Adverb.txt");
	 readFile("Adversary.txt");
	 readFile("Adze.txt");
	 readFile("Aegis.txt");
	 readFile("Aerate.txt");
	 readFile("Affable.txt");
	 readFile("Affluence.txt");
	 readFile("Affront.txt");
	 readFile("Anonymous.txt");
	 readFile("Another.txt");
	 readFile("Answer.txt");
	 readFile("Backgammon.txt");
	 readFile("Bail.txt");
	 readFile("Bairn.txt");
	 readFile("Bald.txt");
	 readFile("Bale.txt");
	 readFile("Benevolent.txt");
	 readFile("Benight.txt");
	 readFile("Bestead.txt");
	 readFile("Bet.txt");
	 readFile("Beverage.txt");
	 readFile("Bewail.txt");
	 readFile("Broil.txt");
	 readFile("Broke.txt");
	 readFile("Brunt.txt");
	 readFile("Buff.txt");
	 readFile("Card.txt");
	 readFile("Career.txt");
	 readFile("Carpet.txt");
	 readFile("Carriage.txt");
	 readFile("Clamp.txt");
	 readFile("Clan.txt");
	 readFile("Clandestine.txt");
	 readFile("Clergy.txt");
	 readFile("Clever.txt");
	 readFile("Codify.txt");
	 readFile("Coeducation.txt");
	 readFile("Coerce.txt");
	 readFile("Cohesion.txt");
	 readFile("Coil.txt");
	 readFile("Collar.txt");
	 readFile("Colloquy.txt");
	 readFile("Collude.txt");
	 readFile("Concession.txt");
	 readFile("Conch.txt");
	 readFile("Concoct.txt");
	 readFile("Concomitant.txt");
	 readFile("Condone.txt");
	 readFile("Debacle.txt");
	 readFile("Debar.txt");
	 readFile("Debase.txt");
	 readFile("Decay.txt");
	 readFile("Decease.txt");
	 readFile("Decide.txt");
	 readFile("Deck.txt");
	 readFile("Declamation.txt");
	 readFile("Destiny.txt");
	 readFile("Destitute.txt");
	 readFile("Desultory.txt");
	 readFile("Detach.txt");
	 readFile("Disable.txt");
	 readFile("Disaffection.txt");
	 readFile("Disclaim.txt");
	 readFile("Discomfort.txt");
	 readFile("Dryad.txt");
	 readFile("Ductile.txt");
	 readFile("Duke.txt");
	 readFile("Duly.txt");
	 readFile("Ebb.txt");
	 readFile("Ebony.txt");
	 readFile("Eclectic.txt");
	 readFile("Ecstasy.txt");
	 readFile("Edible.txt");
	 readFile("Eject.txt");
	 readFile("Elaborate.txt");
	 readFile("Empower.txt");
	 readFile("Encamp.txt");
	 readFile("Enclave.txt");
	 readFile("Endear.txt");
	 readFile("Equine.txt");
	 readFile("Erasure.txt");
	 readFile("Erode.txt");
	 readFile("Eschew.txt");
	 readFile("Escort.txt");
	 readFile("Espirit.txt");
	 readFile("Expatiate.txt");
	 readFile("Expunge.txt");
	 readFile("Fallible.txt");
	 readFile("Familiarize.txt");
	 readFile("Fanfare.txt");
	 readFile("Fascinate.txt");
	 readFile("Flash.txt");
	 readFile("Flaunt.txt");
	 readFile("Flay.txt");
	 readFile("Flimsy.txt");
	 readFile("Flog.txt");
	 readFile("Glutton.txt");
	 readFile("Goblin.txt");
	 readFile("Goose.txt");
	 readFile("Guise.txt");
	 readFile("Gullible.txt");
	 readFile("Gunny.txt");
	 readFile("Gynarchy.txt");
	 readFile("Happy.txt");
	 readFile("Hem.txt");
	 readFile("Henpecked.txt");
	 readFile("Herbivorous.txt");
	 readFile("Heretic.txt");
	 readFile("Hern.txt");
	 readFile("Horde.txt");
	 readFile("Horseplay.txt");
	 readFile("Hound.txt");
	 readFile("Icicle.txt");
	 readFile("Iconoclast.txt");
	 readFile("Ideology.txt");
	 readFile("Ignorance.txt");
	 readFile("Imperial.txt");
	 readFile("Impinge.txt");
	 readFile("Imposition.txt");
	 readFile("Incredulous.txt");
	 readFile("Incurious.txt");
	 readFile("Indelicate.txt");
	 readFile("Indemnify.txt");
	 readFile("Inquiry.txt");
	 readFile("Inquisitive.txt");
	 readFile("Inroad.txt");
	 readFile("Insidious.txt");
	 readFile("Insight.txt");
	 readFile("Insoluble.txt");
	 readFile("Involve.txt");
	 readFile("Invulnerable.txt");
	 readFile("Jump.txt");
	 readFile("Junction.txt");
	 readFile("Jurisprudence.txt");
	 readFile("Jurist.txt");
	 readFile("Jute.txt");
	 readFile("Juvenile.txt");
	 readFile("Lambative.txt");
	 readFile("Lambent.txt");
	 readFile("Lancet.txt");
	 readFile("Landgravine.txt");
	 readFile("Landholder.txt");
	 readFile("Larynx.txt");
	 readFile("Lascar.txt");
	 readFile("Lassitude.txt");
	 readFile("Litigation.txt");
	 readFile("Litre.txt");
	 readFile("Loaf.txt");
	 readFile("Lobby.txt");
	 readFile("Lodestone.txt");
	 readFile("Maiden.txt");
	 readFile("Mail.txt");
	 readFile("Malign.txt");
	 readFile("Misconstrue.txt");
	 readFile("Myopia.txt");
	 readFile("Myriad.txt");
	 readFile("Myth.txt");
	 readFile("Nominee.txt");
	 readFile("Nonage.txt");
	 readFile("Offal.txt");
	 readFile("Offence.txt");
	 readFile("Outhouse.txt");
	 readFile("Outing.txt");
	 readFile("Outspoken.txt");
	 readFile("Papyrus.txt");
	 readFile("Parcel.txt");
	 readFile("Parish.txt");
	 readFile("Pluck.txt");
	 readFile("Plumage.txt");
	 readFile("Pod.txt");
	 readFile("Proclivity.txt");
	 readFile("Recite.txt");
	 readFile("Reck.txt");
	 readFile("Slovenly.txt");
	 readFile("Slush.txt");
	 readFile("Spasmodic.txt");
	 readFile("Spite.txt");
	 readFile("Staunch.txt");
	 readFile("Suavity.txt");
	 readFile("Talisman.txt");
	 readFile("Taper.txt");
	 readFile("Tomboy.txt");
	 readFile("Torchbearer.txt");
	 readFile("Trowel.txt");
	 readFile("Tuck.txt");
	 readFile("Untoward.txt");
	 readFile("Upright.txt");
	 readFile("Wager.txt");
	 readFile("Wallaby.txt");
	 readFile("Wane.txt");
	 readFile("Wilful.txt");
	 readFile("Winsome.txt");
	 readFile("Zealous.txt");
	 readFile("Zymotic.txt");
    }

 }


 // Method for getting host address
 private static String getHostAddress(){
	 try {
		   InetAddress addr = InetAddress.getLocalHost();
		   	return (addr.getHostAddress());
		 } catch (UnknownHostException e) {
			 return null;
		 }
 }

//RMI Method for writing files

 public static void writeFile(String FileName, String Contents){

	  try {

		   TTransport SuperNodeTransport;


		   String supernodeAddr = "csel-x29-10";
		   if(USE_LOCAL) supernodeAddr = "localhost";
		   SuperNodeTransport = new TSocket(supernodeAddr, 9090); // csel-x29-10
		   SuperNodeTransport.open();



		   TProtocol SuperNodeProtocol = new TBinaryProtocol(SuperNodeTransport);
		   SuperNodeService.Client supernodeclient = new SuperNodeService.Client(SuperNodeProtocol);
		   NodeName ndi = strToNodeName(supernodeclient.GetNode());
		   SuperNodeTransport.close();


		   TTransport NodeTransport;
		   System.out.println("Connecting to: " + ndi.getIP() + ":" + ndi.getPort()+":"+ ndi.getID());
		   NodeTransport = new TSocket(ndi.getIP(), ndi.getPort());
		   NodeTransport.open();

		   TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
		   NodeService.Client nodeclient = new NodeService.Client(NodeProtocol);

		   if(nodeclient.Write(FileName, Contents) == true){
			  System.out.println("write to File "+ FileName+" successful...");
		   }
		   else{
			   System.out.println("write to File "+ FileName+" NOT successful...");
		   }

		   NodeTransport.close();


		  }
		  catch (TException x) {
		   x.printStackTrace();
		  }

 }


// RMI Method for reading files
public static void readFile(String FileName){

	try {
		   TTransport SuperNodeTransport;

		   String supernodeAddr = "csel-x29-10";
		   if(USE_LOCAL) supernodeAddr = "localhost";
		   SuperNodeTransport = new TSocket(supernodeAddr, 9090); // csel-x29-10
		   SuperNodeTransport.open();

		   TProtocol SuperNodeProtocol = new TBinaryProtocol(SuperNodeTransport);
		   SuperNodeService.Client supernodeclient = new SuperNodeService.Client(SuperNodeProtocol);
		   NodeName ndi = strToNodeName(supernodeclient.GetNode());
		   SuperNodeTransport.close();

		   System.out.println("Connecting to: " + ndi.getIP() + ":" + ndi.getPort()+":"+ndi.getID());

		   TTransport NodeTransport;
		   NodeTransport = new TSocket(ndi.getIP(), ndi.getPort());
		   NodeTransport.open();

		   TProtocol NodeProtocol = new TBinaryProtocol(NodeTransport);
		   NodeService.Client nodeclient = new NodeService.Client(NodeProtocol);

		   System.out.println("File Content of "+FileName +" returned from the Client is "+nodeclient.Read(FileName));


		   NodeTransport.close();

		  }
		  catch (TException x) {
		   x.printStackTrace();
		  }
		 }
}
