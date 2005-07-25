
/*----------------------------------------------------------------------

Toy EDProducers and EDProducts for testing purposes only.

----------------------------------------------------------------------*/

#include <stdexcept>
#include <string>
#include <iostream>

#include "FWCore/Framework/interface/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "FWCore/Framework/interface/MakerMacros.h"

#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

#include "CondFormats/Calibration/interface/Pedestals.h"
#include "CondFormats/DataRecord/interface/CalibrationRecord.h"

using namespace std;

namespace edmtest
{
  class TrackerCalibAnalyzer : public edm::EDAnalyzer
  {
  public:
    explicit TrackerCalibAnalyzer(edm::ParameterSet const& p) 
    { }
    explicit TrackerCalibAnalyzer(int i) 
    { }
    virtual ~TrackerCalibAnalyzer() { }
    virtual void analyze(const edm::Event& e, const edm::EventSetup& c);
  private:
  };
  
  void
  TrackerCalibAnalyzer::analyze(const edm::Event& e, const edm::EventSetup& context)
  {
    using namespace edm::eventsetup;
    // Context is not used.
    std::cout <<" I AM IN THE EVENT LOOP "<<e.id() <<std::endl;
    edm::eventsetup::ESHandle<Pedestals> ped;
    context.get<CalibrationRecord>().get(ped);
    //call tracker code
    //
    std::cout <<" Tracker ped for 100 "<<std::endl;
    /*
    Pedestals::ItemIterator it;
    Pedestals::ItemIterator itbeg=(*ped).m_pedestals.begin();
    Pedestals::ItemIterator itend=(*ped).m_pedestals.end();
    int counter=0;
    for(it=itbeg; it!=itend; ++it){
      ++counter;
      if(counter<=100){
	std::cout << "mean " << (*it).m_mean << ",variance" << (*it).m_variance <<std::endl;
      }       
    }
    */
  }
  DEFINE_FWK_MODULE(TrackerCalibAnalyzer)
}
