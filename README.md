# D-Stream
Implementation of the D-Stream clustering algorithm for use in MOA.

<u>TODO<u>:
1. Complete 'adjustClustering' method in DStream;
2. Write 'getClusteringResult' method in DStream;
3. Add functionality to check whether it is faster, when assigning a grid to a cluster, if it is faster to iterate through grid_list or through that grid's neighbours;
4. Ensure that 'assignGridtoCluster' accounts for all boundary conditions; and
5. Generate Javadoc.

The D-Stream algorithm is described by Yixin Chen and Li Tu in their paper "Density-Based Clustering for Real-Time Stream Data" [2]. Please cite that paper if you use this code.

MOA (Massive Online Analysis) [1] is a Java-based, open source framework for data stream mining. More details can be found on its website (http://moa.cms.waikato.ac.nz/) and it can be found on GitHub as well (https://github.com/waikato/moa).

[1] A. Bifet, G. Holmes, R. Kirkby, and B. Pfahringer, “Moa: Massive online analysis,” J. Mach. Learn. Res., vol. 11, no. May, pp. 1601–1604, 2010.

[2] Y. Chen and L. Tu, “Density-Based Clustering for Real-Time Stream Data,” in Proceedings of the 13th ACM SIGKDD international conference on Knowledge discovery and data mining, 2007, pp. 133–142.
