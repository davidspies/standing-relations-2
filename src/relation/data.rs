use std::sync::{atomic::AtomicUsize, Arc};

pub(crate) struct RelationData {
    pub(crate) name: Option<String>,
    pub(crate) type_name: &'static str,
    pub(crate) hidden: bool,
    pub(crate) children: Vec<Arc<RelationData>>,
    pub(crate) visit_count: Arc<AtomicUsize>,
}
impl RelationData {
    pub(crate) fn new(type_name: &'static str, children: Vec<Arc<RelationData>>) -> Self {
        Self {
            name: None,
            type_name,
            hidden: false,
            children,
            visit_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn on_first_shown(&mut self, f: impl FnOnce(&mut Self)) {
        if self.hidden {
            assert_eq!(self.children.len(), 1);
            let child = Arc::get_mut(&mut self.children[0]).unwrap();
            child.on_first_shown(f)
        } else {
            f(self)
        }
    }

    pub(super) fn set_name(&mut self, name: String) {
        self.on_first_shown(|data| data.name = Some(name))
    }

    pub(super) fn set_type_name(&mut self, type_name: &'static str) {
        self.on_first_shown(|data| data.type_name = type_name)
    }

    pub(super) fn hide(&mut self) {
        self.on_first_shown(|data| data.hidden = true)
    }
}
