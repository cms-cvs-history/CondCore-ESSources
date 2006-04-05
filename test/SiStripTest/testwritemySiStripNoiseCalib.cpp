#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CondFormats/Calibration/interface/mySiStripNoises.h"
#include <stdexcept>
#include <string>
#include <iostream>
#include <boost/random/linear_congruential.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>
typedef boost::minstd_rand base_generator_type;
int main(int argc, char *argv[]){
  std::string connect("sqlite_file:mydb_Noise.db");
  std::string mappingFile("mySiStripNoises-mapping-custom-1.0.xml");
  size_t numberOfObj=2;
  unsigned int bsize=1000;
  unsigned int detidseed=1234;
  //unsigned int nstrips=255;
  unsigned int nstrips=5;
  unsigned int nAPV=2;
  cond::ServiceLoader* loader=new cond::ServiceLoader;
  std::cout<<"loading XMLAuthenticationService"<<std::endl;
  loader->loadAuthenticationService(cond::XML);
  std::cout<<"loading MessageServic"<<std::endl;
  loader->loadMessageService(cond::Info);
  std::cout<<"done"<<std::endl;
  std::cout<<"loading BlobStreamingService"<<std::endl;
  loader->loadBlobStreamingService();
  std::cout<<"done"<<std::endl;
  cond::DBSession* session=new cond::DBSession(connect);
  session->setCatalog( "file:mycat.xml" );

  // Define a uniform random number distribution which produces "double"
  // values between 0 and 1 (0 inclusive, 1 exclusive).
 
  base_generator_type rng(42u);
  boost::uniform_real<> uni_dist(0,1);
  boost::variate_generator<base_generator_type&, boost::uniform_real<> > uni(rng, uni_dist);
  std::string noiseiovtok;
  try{
    session->connect(cond::ReadWriteCreate);
    std::cout << "##### processing custom mapping " << std::endl;
    cond::DBWriter pwriter(*session, "BlobNoiseContainer",mappingFile);
    cond::DBWriter iovwriter(*session, "IOV");
    cond::IOV* noiseiov=new cond::IOV;
    //unsigned int i;
    session->startUpdateTransaction();
    for (unsigned int i = 0; i <numberOfObj ; ++i ){
      std::cout<<"obj number "<<i<<std::endl;
      mySiStripNoises* me = new mySiStripNoises;
      for (uint32_t detid=detidseed;detid<(detidseed+bsize);detid++){
	//Generate Noise for det detid
	std::cout << "encoding data for detid " << detid << std::endl;
	mySiStripNoises::SiStripNoiseVector theSiStripVector;
	mySiStripNoises::SiStripData theSiStripData;
	for(unsigned short j=0; j<nAPV; ++j){
	  for(unsigned int strip=0; strip<nstrips; ++strip){
	    float noiseValue=uni();
	    std::cout<<"\t encoding noise value "<<noiseValue<<std::endl;
	    theSiStripData.setData(noiseValue,false);
	    theSiStripVector.push_back(theSiStripData.Data);
	    std::cout<<"\t encoding noise as short "<<theSiStripData.Data<<std::endl;
	  }
	}
	mySiStripNoises::Range range(theSiStripVector.begin(),theSiStripVector.end());
	bool putstat=me->put(detid,range);
	if(putstat){
	  std::cout<<"put range ok"<<std::endl;
	}else{
	  std::cout<<"detid already exists"<<std::endl;
	}
      }
      //checking transient 
      std::vector<uint32_t> DetIds;
      me->getDetIds(DetIds);
      for (size_t idet=0;idet<DetIds.size();idet++){
	std::cout << "decoding for detid " << DetIds[idet] << std::endl;
	const mySiStripNoises::Range range = me->getRange( DetIds[idet] );
	mySiStripNoises::ContainerIterator iter=range.first;
	for (;iter!=range.second;iter++){
	  std::cout << "\t Data " <<  *iter << std::endl;
	  mySiStripNoises::SiStripData d;
	  d.setData(*iter);
	  std::cout<<"\t Noise "<<d.getNoise()<<" - "<<d.getDisable()<<std::endl;
	}
      }
      std::cout<<"Writing object "<<std::endl;
      std::string mytok=pwriter.markWrite<mySiStripNoises>(me);
      noiseiov->iov.insert(std::make_pair(i+1,mytok));
    }
    std::cout<<"Writing IOV object "<<std::endl;
    noiseiovtok=iovwriter.markWrite<cond::IOV>(noiseiov); 
    std::cout<<"done"<<std::endl;
    session->commit();
    session->disconnect(); 
  }catch( const pool::Exception& e ){
    std::cout<<e.what()<<std::endl;
  }catch (const cond::Exception& e){
    std::cout<<e.what()<<std::endl;
  }catch (const std::exception& e){
    std::cout<<e.what()<<std::endl;
  }
  delete session;
  cond::MetaData mymetadata_svc(connect,*loader);
  mymetadata_svc.connect();
  mymetadata_svc.addMapping("blobtest", noiseiovtok);
  mymetadata_svc.disconnect();
  delete loader;
}
