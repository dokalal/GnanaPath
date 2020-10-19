(function(){
  document.addEventListener('DOMContentLoaded', function(){
    let $$ = selector => Array.from( document.querySelectorAll( selector ) );
    let $ = selector => document.querySelector( selector );

    let tryPromise = fn => Promise.resolve().then( fn );

    let toJson = obj => obj.json();
    let toText = obj => obj.text();

    let cy;

    let $stylesheet = "style.json";
    let getStylesheet = name => {
      let convert = res => name.match(/[.]json$/) ? toJson(res) : toText(res);

      return fetch(`/static/cola/stylesheets/${name}`).then( convert );
    };
    let applyStylesheet = stylesheet => {
      if( typeof stylesheet === typeof '' ){
        cy.style().fromString( stylesheet ).update();
      } else {
        cy.style().fromJson( stylesheet ).update();
      }
    };
    let applyStylesheetFromSelect = () => Promise.resolve( $stylesheet ).then( getStylesheet ).then( applyStylesheet );


      
    let $dataset = "jdk_dependency.json";
    let getDataset = name => fetch(`datasets/${name}`).then( toJson );
    let getGnDataset = name => gnFetchData();
      
    let applyDataset = dataset => {
      // so new eles are offscreen
      cy.zoom(0.001);
      cy.pan({ x: -9999999, y: -9999999 });
      console.log('View: new Dataset is applied');
      // replace eles
      cy.elements().remove();
      cy.add( dataset );
    }
      
    let applyDatasetFromSelect = () => 
          Promise.resolve( $dataset ).then( getGnDataset ).then( applyDataset );

    ///let applyDatasetFromSelect = () => 
	///Promise.resolve( $dataset).then( getGnDataSet().then(
	    
    let $layout = "cola";
    let maxLayoutDuration = 1500;
    let layoutPadding = 50;
    let layouts = {
      cola: {
        name: 'cola',
        padding: layoutPadding,
        nodeSpacing: 12,
        edgeLengthVal: 45,
        animate: true,
        randomize: true,
        maxSimulationTime: maxLayoutDuration,
        boundingBox: { // to give cola more space to resolve initial overlaps
          x1: 0,
          y1: 0,
          x2: 10000,
          y2: 10000
        },
        edgeLength: function( e ){
          let w = e.data('weight');

          if( w == null ){
            w = 0.5;
          }

          return 45 / w;
        }
      }
    };
      
    console.log('View: Init called');  
    let prevLayout;
    let getLayout = name => Promise.resolve( layouts[ name ] );
    let applyLayout = layout => {
      if( prevLayout ){
        prevLayout.stop();
      }
      console.log('View: Apply New Layout');
      let l = prevLayout = cy.makeLayout( layout );
      return l.run().promiseOn('layoutstop');
    }
      
    let applyLayoutFromSelect = () => Promise.resolve( $layout ).then( getLayout ).then( applyLayout );

    let $algorithm = $('#algorithm');
    let getAlgorithm = (name) => {
      switch (name) {
        case 'bfs': return Promise.resolve(cy.elements().bfs.bind(cy.elements()));
        case 'dfs': return Promise.resolve(cy.elements().dfs.bind(cy.elements()));
        case 'none': return Promise.resolve(undefined);
        default: return Promise.resolve(undefined);
      }
    };
    let runAlgorithm = (algorithm) => {
      if (algorithm === undefined) {
        return Promise.resolve(undefined);
      } else {
        let options = {
          root: '#' + cy.nodes()[0].id(),
          // astar requires target; goal property is ignored for other algorithms
          goal: '#' + cy.nodes()[Math.round(Math.random() * (cy.nodes().size() - 1))].id()
        };
        return Promise.resolve(algorithm(options));
      }
    }
    let currentAlgorithm;
    let animateAlgorithm = (algResults) => {
      // clear old algorithm results
      cy.$().removeClass('highlighted start end');
      currentAlgorithm = algResults;
      if (algResults === undefined || algResults.path === undefined) {
        return Promise.resolve();
      }
      else {
        let i = 0;
        // for astar, highlight first and final before showing path
        if (algResults.distance) {
          // Among DFS, BFS, A*, only A* will have the distance property defined
          algResults.path.first().addClass('highlighted start');
          algResults.path.last().addClass('highlighted end');
          // i is not advanced to 1, so start node is effectively highlighted twice.
          // this is intentional; creates a short pause between highlighting ends and highlighting the path
        }
        return new Promise(resolve => {
          let highlightNext = () => {
            if (currentAlgorithm === algResults && i < algResults.path.length) {
              algResults.path[i].addClass('highlighted');
              i++;
              setTimeout(highlightNext, 50);
            } else {
              // resolve when finished or when a new algorithm has started visualization
              resolve();
            }
          }
          highlightNext();
        });
      }
    };
    let applyAlgorithmFromSelect = () => Promise.resolve( $algorithm.value ).then( getAlgorithm ).then( runAlgorithm ).then( animateAlgorithm );

    cy = window.cy = cytoscape({
      container: $('#cy')
    });

      tryPromise( applyDatasetFromSelect ).then( applyStylesheetFromSelect ).then( applyLayoutFromSelect );

      let applyDatasetChange = () => tryPromise(applyDatasetFromSelect).then(applyStylesheetFromSelect).then(applyLayoutFromSelect);
      
    $('#redo-layout').addEventListener('click', applyLayoutFromSelect);

    $algorithm.addEventListener('change', applyAlgorithmFromSelect);
    $('#redo-algorithm').addEventListener('click', applyAlgorithmFromSelect);

    //////////////////////////////////
    $('#srchbtn').addEventListener('click', applyDatasetChange);
      
    function  gnFetchData() {

        //var srchstr = "SELECT * from customer;";
	///var srv = 'http://45.79.206.248:5050';	
	///var url = '/static/cola/js/product.json';
	var srchstr = document.getElementById('srchid').value;

	console.log('GNView: sql txt srch '+srchstr);
	var url = "/api/v1/search?srchqry='"+srchstr+"'";
	
	var myHeaders = new Headers({
	    'Content-Type': 'text/plain',
	    'Access-Control-Allow-Origin' : '*'
	});
	
	console.log('GNView: Fetch data1 ');
	return fetch(url,
		     {
			 method: 'GET',
			 headers: myHeaders
		     }
		    )
          .then (response => response.json())
          .then (data => {
             console.log('GNView: Fetch complete ');
             console.log('GNView: data:'+JSON.stringify(data, null,3));
             var nodelen = data.nodes.length;

             var   s = '';

            ///s = '{'+"\n";
            s += '['+"\n";
            console.log('GNView: fetch complete len:'+nodelen);
            for (i=0; i < nodelen; i++) {

                if ( i > 0)
                    s+= ",";

                s += '{';
		n = data.nodes[i];
                s += '"data": {';

                ///console.log('GNView node-'+i+' id '+n.id);
                s+= '"id": "'+n.id+'", ';
		s+= '"idInt": '+(i+1)+',';
		s+= '"name": "'+n.id+'", ';
		s+= '"query": true ';
                s+= '}}';

            }
            s += ']'+"\n";
            ////s += '}'+"\n";
            console.log('GNView nodes:  '+s);
            gn_elements = JSON.parse(s);
	      //gn_elements  = s;
            console.log('GnanaView: Fetch issued :');
              return (gn_elements);
          });
    }


      ////////////////////////////////

      
  });
})();

// tooltips with jQuery
$(document).ready(() => $('.tooltip').tooltipster());