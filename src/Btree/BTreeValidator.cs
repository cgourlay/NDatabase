using NDatabase.Api;
using NDatabase.Exceptions;

namespace NDatabase.Btree
{
    internal static class BTreeValidator
    {
        public static void CheckDuplicateChildren(IBTreeNode node1, IBTreeNode node2)
        {
            if (!OdbConfiguration.IsBTreeValidationEnabled())
            {
                return;
            }

            for (int i = 0; i < node1.GetNbChildren(); i++)
            {
                IBTreeNode child1 = node1.GetChildAt(i, true);

                for (int j = 0; j < node2.GetNbChildren(); j++)
                {
                    if (child1 == node2.GetChildAt(j, true))
                    {
                        throw new BTreeNodeValidationException("Duplicated node : " + child1);
                    }
                }
            }
        }

        public static void ValidateNode(IBTreeNode node, bool isRoot)
        {
            if (!OdbConfiguration.IsBTreeValidationEnabled())
            {
                return;
            }

            ValidateNode(node);

            if (isRoot && node.HasParent())
            {
                throw new BTreeNodeValidationException("Root node with a parent: " + node);
            }

            if (!isRoot && !node.HasParent())
            {
                throw new BTreeNodeValidationException("Internal node without parent: " + node);
            }
        }

        public static void ValidateNode(IBTreeNode node)
        {
            if (!OdbConfiguration.IsBTreeValidationEnabled())
            {
                return;
            }

            int nbKeys = node.GetNbKeys();
            if (node.HasParent() && nbKeys < node.GetDegree() - 1)
            {
                string degree = (node.GetDegree() - 1).ToString();
                throw new BTreeNodeValidationException("Node with less than " + degree + " keys");
            }

            int maxNbKeys = node.GetDegree()*2 - 1;
            int nbChildren = node.GetNbChildren();
            int maxNbChildren = node.GetDegree()*2;

            if (nbChildren != 0 && nbKeys == 0)
            {
                throw new BTreeNodeValidationException("Node with no key but with children : " + node);
            }

            for (int i = 0; i < nbKeys; i++)
            {
                if (node.GetKeyAndValueAt(i) == null)
                {
                    string keyIndex = i.ToString();
                    throw new BTreeNodeValidationException("Null key at " + keyIndex + " on node " + node);
                }

                CheckValuesOfChild(node.GetKeyAndValueAt(i), node.GetChildAt(i, false));
            }

            for (int i = nbKeys; i < maxNbKeys; i++)
            {
                if (node.GetKeyAndValueAt(i) != null)
                {
                    throw new BTreeNodeValidationException(string.Concat("Not Null key at ", i.ToString(),
                        " on node " + node));
                }
            }

            IBTreeNode previousNode = null;

            for (int i = 0; i < nbChildren; i++)
            {
                if (node.GetChildAt(i, false) == null)
                {
                    throw new BTreeNodeValidationException(string.Concat("Null child at index ", i.ToString(),
                        " on node " + node));
                }

                if (previousNode != null && previousNode == node.GetChildAt(i, false))
                {
                    throw new BTreeNodeValidationException(string.Concat("Two equals children at index ", i.ToString(),
                        " : " + previousNode));
                }

                previousNode = node.GetChildAt(i, false);
            }

            for (int i = nbChildren; i < maxNbChildren; i++)
            {
                if (node.GetChildAt(i, false) != null)
                {
                    throw new BTreeNodeValidationException(string.Concat("Not Null child at ", i.ToString(),
                        " on node " + node));
                }
            }
        }

        private static void CheckValuesOfChild(IKeyAndValue key, IBTreeNode node)
        {
            if (!OdbConfiguration.IsBTreeValidationEnabled())
            {
                return;
            }

            if (node == null)
            {
                return;
            }

            for (int i = 0; i < node.GetNbKeys(); i++)
            {
                if (node.GetKeyAndValueAt(i).GetKey().CompareTo(key.GetKey()) >= 0)
                {
                    throw new BTreeNodeValidationException("Left child with values bigger than pivot " + key + " : " +
                                                           node);
                }
            }
        }
    }
}