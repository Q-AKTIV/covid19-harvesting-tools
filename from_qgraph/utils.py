""" Utility functions for (multiple) harvesting scripts """


def strip_mesh_qualifier(meshterm):
    """ Strip off qualifier terms from mesh terms,
    which are usually appended with a slash '/' """
    return meshterm.split('/')[0].strip()
